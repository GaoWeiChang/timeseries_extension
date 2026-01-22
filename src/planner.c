#include <postgres.h>
#include <optimizer/planner.h>
#include <optimizer/pathnode.h>
#include <nodes/pg_list.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/hsearch.h>
#include <utils/inval.h>
#include <executor/spi.h>
#include <nodes/makefuncs.h>
#include <parser/parsetree.h>
#include <access/xact.h>

#include "metadata.h"

/*
    Cache system for planner Workflow

    [Transaction Start]
            ↓
    [User perform DDL CREATE/DROP hypertable]
            ↓
    [Transaction COMMIT/ABORT]
            ↓
    [Invoke invalidate_cache_callback] => invalidate the cache after COMMIT, since it might not the same as catalog that commited
            ↓
    [cache_valid = false]
            ↓
    [User run SELECT query]
            ↓
    [Call is_hypertable_relation() from planner]
            ↓
    [Check is cache invalid]
            ↓ true
    [rebuild_hypertable_cache()] => directly read from catalog (table_open)
            ↓
    [cache_valid = true]
            ↓
    [Find in cache]
            ↓
    [Return result]
*/

static planner_hook_type prev_planner_hook = NULL;

typedef struct HypertableCacheKey
{
    Oid relid;
} HypertableCacheKey;

typedef struct HypertableCacheEntry
{
    Oid relid; // key
    bool is_hypertable; // value
    char schema_name[NAMEDATALEN];
    char table_name[NAMEDATALEN];
} HypertableCacheEntry;

static HTAB *hypertable_cache = NULL;
static bool cache_valid = false;

static void init_hypertable_cache(void);
static void rebuild_hypertable_cache(void);
static void invalidate_cache_callback(XactEvent event, void *arg);

// initialize the cache hash table
static void 
init_hypertable_cache(void)
{
    HASHCTL ctl;

    if(hypertable_cache != NULL){
        return;
    }

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(HypertableCacheKey);
    ctl.entrysize = sizeof(HypertableCacheEntry);
    ctl.hcxt = TopMemoryContext; // cache must in TopMemoryContext

    hypertable_cache = hash_create("Hypertable Cache",
                                   256,
                                   &ctl,
                                   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    
    elog(LOG, "Hypertable cache initialized");
}

//  rebuild cache from catalog
static void 
rebuild_hypertable_cache(void)
{
    int ret;
    HASH_SEQ_STATUS status;
    HypertableCacheEntry *entry;

    if(hypertable_cache == NULL){
        init_hypertable_cache();
    }

    hash_seq_init(&status, hypertable_cache);
    while ((entry = (HypertableCacheEntry *) hash_seq_search(&status)) != NULL)
    {
        hash_search(hypertable_cache, &entry->relid, HASH_REMOVE, NULL);
    }

    // check _timeseries_catalog schema exists
    Oid catalog_schema_oid = get_namespace_oid("_timeseries_catalog", true);
    if(catalog_schema_oid == InvalidOid){
        elog(LOG, "Schema _timeseries_catalog does not exist, cache empty");
        cache_valid = true;
        return;
    }
    
    // load all hypertable from catalog
    SPI_connect();

    ret = SPI_execute("SELECT schema_name, table_name FROM _timeseries_catalog.hypertable", 
                    true, 0);
    
    if (ret == SPI_OK_SELECT && SPI_processed > 0){
        for (uint64 i = 0; i < SPI_processed; i++){
            char *schema_name;
            char *table_name;
            Oid schema_oid;
            Oid table_oid;
            bool isnull;
            Datum datum;
            HypertableCacheEntry *cache_entry;
            HypertableCacheKey key;
            bool found;

            // schema_name 
            datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
            if (isnull) continue;
            schema_name = TextDatumGetCString(datum);

            // table_name
            datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);
            if (isnull) continue;
            table_name = TextDatumGetCString(datum);

            // oid 
            schema_oid = get_namespace_oid(schema_name, true);
            if (schema_oid == InvalidOid) continue;

            table_oid = get_relname_relid(table_name, schema_oid);
            if (table_oid == InvalidOid) continue;
            
            // add to cache
            key.relid = table_oid;
            cache_entry = (HypertableCacheEntry *) hash_search(hypertable_cache,
                                                                &key,
                                                                HASH_ENTER,
                                                                &found);
            
            cache_entry->relid = table_oid;
            cache_entry->is_hypertable = true;
            strncpy(cache_entry->schema_name, schema_name, NAMEDATALEN);
            strncpy(cache_entry->table_name, table_name, NAMEDATALEN);

            elog(LOG, "Added to cache: %s.%s (OID: %u)", schema_name, table_name, table_oid);
        }
    }

    SPI_finish();
    cache_valid = true;

    elog(LOG, "Hypertable cache rebuilt with %lu entries",  hash_get_num_entries(hypertable_cache));
}

// Transaction callback to invalidate cache
static void 
invalidate_cache_callback(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_COMMIT: // commit
        case XACT_EVENT_ABORT: // when roolback/abort
            cache_valid = false;
            // elog(NOTICE, "Hypertable cache invalidated");
            break;
        default:
            break;
    }
}

static bool 
is_hypertable_relation(RangeTblEntry *rte)
{
    HypertableCacheKey key;
    HypertableCacheEntry *entry;
    bool found;

    if (rte->rtekind != RTE_RELATION){
        return false;
    }

    char *schema_name = get_namespace_name(get_rel_namespace(rte->relid));
    if (schema_name && strcmp(schema_name, "_timeseries_catalog") == 0){
        return false;
    }

    if (hypertable_cache == NULL){
        init_hypertable_cache();
    }

    if (!cache_valid){
        rebuild_hypertable_cache();
    }

    key.relid = rte->relid;
    entry = (HypertableCacheEntry *) hash_search(hypertable_cache,
                                                &key,
                                                HASH_FIND,
                                                &found);

    if (found && entry->is_hypertable){
        elog(LOG, "Cache hit: %s.%s is a hypertable", 
             entry->schema_name, entry->table_name);
        return true;
    }

    return false;
}

static PlannedStmt *
timeseries_planner_hook(Query *parse,
                       const char *query_string,
                       int cursorOptions,
                       ParamListInfo boundParams)
{
    PlannedStmt *result;
    RangeTblEntry *rte;

    if((parse->commandType == CMD_SELECT) && (list_length(parse->rtable) > 0)){
        rte = (RangeTblEntry *) linitial(parse->rtable);
        if (is_hypertable_relation(rte)){
            char *schema_name = get_namespace_name(get_rel_namespace(rte->relid));
            char *table_name = get_rel_name(rte->relid);
            
            elog(LOG, "Planner: Optimizing query on hypertable %s.%s", schema_name, table_name);
        }
    }

    // call previous hook or standard planner
    if(prev_planner_hook && prev_planner_hook != timeseries_planner_hook){
        result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    }
    else{
        result = standard_planner(parse, query_string, cursorOptions, boundParams);
    }
    
    return result;
}


void
planner_hook_init(void)
{
    // prevent double installation 
    if (planner_hook == timeseries_planner_hook){
        elog(WARNING, "Timeseries planner hook already installed");
        return;
    }

    init_hypertable_cache();

    // register transaction callback 
    RegisterXactCallback(invalidate_cache_callback, NULL);

    // install planner hook
    prev_planner_hook = planner_hook;
    planner_hook = timeseries_planner_hook;
    
    elog(LOG, "Timeseries planner hook installed");
}

void
planner_hook_cleanup(void)
{
    // unregister transaction callback
    UnregisterXactCallback(invalidate_cache_callback, NULL);

    // remove planner hook
    if (planner_hook == timeseries_planner_hook){
        planner_hook = prev_planner_hook;
        elog(LOG, "Timeseries planner hook removed");
    }

    // clean up cache
    if (hypertable_cache != NULL){
        hash_destroy(hypertable_cache);
        hypertable_cache = NULL;
        cache_valid = false;
        elog(LOG, "Hypertable cache destroyed");
    }
}

void
planner_invalidate_cache(void){
    cache_valid = false;
}
