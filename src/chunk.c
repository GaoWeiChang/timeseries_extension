#include <postgres.h>
#include <fmgr.h>
#include <access/table.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <commands/tablecmds.h>
#include <nodes/parsenodes.h>
#include <parser/parse_node.h>
#include <parser/parse_utilcmd.h>
#include <tcop/utility.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <utils/memutils.h>
#include <utils/hsearch.h>
#include <executor/spi.h>

#include "metadata.h"
#include "chunk.h"

/*
 * Chunk cache management
 */
static HTAB *chunk_cache = NULL;
static MemoryContext chunk_cache_context = NULL;
static bool xact_callback_registered = false;

// reset pointer when transaction finished
static void
chunk_cache_xact_callback(XactEvent event, void *arg)
{
    switch(event){
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_ABORT:
            // reset pointer
            chunk_cache = NULL;
            chunk_cache_context = NULL;
            elog(DEBUG1, "Chunk cache reset");
            break;
        default:
            break;
    }
}

// find chunk in cache
static ChunkInfo*
chunk_cache_search(int hypertable_id, int64 chunk_start)
{
    ChunkCacheKey key;
    ChunkCacheEntry *entry;
    bool found;

    if (chunk_cache == NULL) return NULL;

    key.hypertable_id = hypertable_id;
    key.chunk_start = chunk_start;

    entry = (ChunkCacheEntry *) hash_search(
        chunk_cache,
        &key,
        HASH_FIND,
        &found
    );
    
    if(found){
        // elog(NOTICE, "cache hit: hypertable=%d", hypertable_id);
        return &entry->info;
    }

    // elog(NOTICE, "cache miss: hypertable=%d", hypertable_id);
    return NULL;
}

static void
chunk_cache_insert(int hypertable_id, int64 chunk_start, ChunkInfo *info)
{
    ChunkCacheKey key;
    ChunkCacheEntry *entry;
    bool found;

    if(chunk_cache == NULL){
        chunk_cache_init();
    }

    key.hypertable_id = hypertable_id;
    key.chunk_start = chunk_start;
    
    // search
    entry = (ChunkCacheEntry *) hash_search(
        chunk_cache,
        &key,
        HASH_ENTER,
        &found
    );

    // copy to cache entry
    entry->key = key;
    memcpy(&entry->info, info, sizeof(ChunkInfo));
    elog(DEBUG1, "Chunk cache INSERT: hypertable=%d, start=%ld, chunk=%s",
        hypertable_id, chunk_start, info->table_name);
}

void
chunk_cache_init(void)
{
    HASHCTL ctl;
    
    if (chunk_cache != NULL) return;

    /*
    * when transaction finish memory will get destroy,
    * to avoid dangling pointer for next transaction, we need to reset pointer to NULL
    */  
    if(!xact_callback_registered){
        RegisterXactCallback(chunk_cache_xact_callback, NULL); // reset pointer
        xact_callback_registered = true;
    }

    // memory context for cache, it will reset when transaction finish
    chunk_cache_context = AllocSetContextCreate(
        CurTransactionContext,  // chained with current transaction
        "ChunkCache",
        ALLOCSET_DEFAULT_SIZES
    );

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(ChunkCacheKey);
    ctl.entrysize = sizeof(ChunkCacheEntry);
    ctl.hcxt = chunk_cache_context;

    chunk_cache = hash_create(
        "Chunk Cache",
        64, 
        &ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT
    );

    // elog(NOTICE, "Chunk cache initialized");
}

/*
 * Private fucntion
 */
static int
chunk_get_next_number(int hypertable_id)
{
    StringInfoData query;
    int chunk_number = 1;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT COALESCE(MAX(id), 0) + 1 FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id = %d", hypertable_id);
    
    int ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        ereport(ERROR, errmsg("failed to get next chunk number"));
    }
    
    bool isnull;
    chunk_number = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    
    return chunk_number;
}

static Oid 
chunk_create_table(const char *hypertable_schema,
                   const char *hypertable_name,
                   const char *chunk_schema,
                   const char *chunk_name,
                   const char *time_column,
                   int64 start_time,
                   int64 end_time)
{
    StringInfoData query;
    Oid schema_oid;
    Oid chunk_oid;
    char constraint_name[NAMEDATALEN];

    schema_oid = get_namespace_oid(chunk_schema, false);

    // check table exist
    chunk_oid = get_relname_relid(chunk_name, schema_oid);
    if(chunk_oid != InvalidOid){
        return chunk_oid;
    }

    initStringInfo(&query);
    appendStringInfo(&query,
        "CREATE TABLE IF NOT EXISTS %s.%s (LIKE %s.%s INCLUDING ALL) "
        "INHERITS (%s.%s)",
        chunk_schema, chunk_name, hypertable_schema, hypertable_name,
        hypertable_schema, hypertable_name);
    elog(DEBUG1, "Creating chunk table: %s", query.data);
    
    // create inherit table
    int ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        ereport(ERROR, (errmsg("failed to create chunk table \"%s\"", chunk_name)));
    }

    CommandCounterIncrement();
    chunk_oid = get_relname_relid(chunk_name, schema_oid);

    TimestampTz start_ts = start_time;
    TimestampTz end_ts = end_time;
    
    char *start_str = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(start_ts)));
    char *end_str = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(end_ts)));

    // add constraint
    snprintf(constraint_name, NAMEDATALEN, "%s_time_check", chunk_name);
    resetStringInfo(&query);
    appendStringInfo(&query,
        "ALTER TABLE %s.%s "
        "ADD CONSTRAINT %s "
        "CHECK (%s >= '%s'::timestamptz AND %s < '%s'::timestamptz)",
        chunk_schema, chunk_name,
        constraint_name,
        time_column, start_str,
        time_column, end_str);

    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        ereport(ERROR, errmsg("failed to add time constraint to chunk \"%s\"", chunk_name));
    }

    return chunk_oid;
}

/*
 * Public fucntion
 */
int64 
chunk_calculate_start(int64 time_point, int64 chunk_interval)
{
    if (chunk_interval <= 0){
        ereport(ERROR, errmsg("chunk interval must be positive"));
    }
    return (time_point/chunk_interval) * chunk_interval;
}

int64 
chunk_calculate_end(int64 chunk_start, int64 chunk_interval)
{
    return chunk_start + chunk_interval;
}

void
chunk_drop_all_chunk(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    int ret;

    initStringInfo(&query);
    appendStringInfo(&query,
        "DROP TABLE IF EXISTS %s.%s CASCADE", schema_name, table_name);
    
    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY) {
        ereport(WARNING,
                errmsg("failed to drop chunk table %s.%s", schema_name, table_name));
    }
    
    elog(NOTICE, "Dropped chunk table %s.%s", schema_name, table_name);
}

ChunkInfo*
chunk_get_info(int chunk_id)
{
    StringInfoData query;
    int ret;
    bool isnull;
    Datum datum;
    ChunkInfo *info;

    initStringInfo(&query);
    appendStringInfo(&query, 
        "SELECT schema_name, table_name, start_time, end_time "
        "FROM _timeseries_catalog.chunk "
        "WHERE id = %d", chunk_id);
    
    ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        ereport(ERROR, errmsg("chunk with id %d not found", chunk_id));
    }

    info = (ChunkInfo *) palloc(sizeof(ChunkInfo));
    info->chunk_id = chunk_id;

    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    strncpy(info->schema_name, TextDatumGetCString(datum), NAMEDATALEN);
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
    strncpy(info->table_name, TextDatumGetCString(datum), NAMEDATALEN);
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull);
    info->start_time = DatumGetInt64(datum);
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &isnull);
    info->end_time = DatumGetInt64(datum);

    return info;
}

ChunkInfo* 
chunk_create(int hypertable_id, int64 time_point)
{
    StringInfoData query;
    char *hypertable_schema;
    char *hypertable_name;
    char *time_column;
    int64 chunk_interval;
    int64 chunk_start;
    int64 chunk_end;
    int chunk_number;
    char chunk_name[NAMEDATALEN];
    Oid chunk_oid;
    int chunk_id;

    // fetch hypertable
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT h.schema_name, h.table_name, d.column_name, d.interval_length "
        "FROM _timeseries_catalog.hypertable h "
        "JOIN _timeseries_catalog.dimension d ON h.id = d.hypertable_id "
        "WHERE h.id = %d", hypertable_id);
    
    int ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, errmsg("hypertable with id %d not found", hypertable_id));
    }

    bool isnull;
    Datum datum;

    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    hypertable_schema = pstrdup(TextDatumGetCString(datum));
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
    hypertable_name = pstrdup(TextDatumGetCString(datum));
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull);
    time_column = pstrdup(TextDatumGetCString(datum));
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &isnull);
    chunk_interval = DatumGetInt64(datum);
    
    chunk_start = chunk_calculate_start(time_point, chunk_interval);
    chunk_end = chunk_calculate_end(chunk_start, chunk_interval);

    // build chunk name
    chunk_number = chunk_get_next_number(hypertable_id);
    snprintf(chunk_name, NAMEDATALEN, "_hyper_%d_%d_chunk", hypertable_id, chunk_number);

    chunk_oid = chunk_create_table(hypertable_schema,
                                    hypertable_name,
                                    hypertable_schema, 
                                    chunk_name,
                                    time_column,
                                    chunk_start,
                                    chunk_end);

    CommandCounterIncrement();

    chunk_id = metadata_insert_chunk(hypertable_id,
                                    hypertable_schema,
                                    chunk_name,
                                    chunk_start,
                                    chunk_end);

    CommandCounterIncrement();

    ChunkInfo *info = (ChunkInfo *) palloc(sizeof(ChunkInfo));
    info->chunk_id = chunk_id;
    strcpy(info->schema_name, hypertable_schema);
    strcpy(info->table_name, chunk_name);
    info->start_time = chunk_start;
    info->end_time = chunk_end;

    chunk_cache_insert(hypertable_id, chunk_start, info);
    
    elog(NOTICE, "✅ Chunk %d created successfully (OID: %u)", info->chunk_id, chunk_oid);
    return info;
}

ChunkInfo* 
chunk_get_or_create(int hypertable_id, int64 timestamp)
{
    int64 chunk_interval;
    int64 chunk_start;
    int64 lock_key;
    ChunkInfo *cached_info;
    int chunk_id;

    if (chunk_cache == NULL){
        chunk_cache_init();
    }

    // calculate chunk start
    chunk_interval = metadata_get_chunk_interval(hypertable_id);
    if (chunk_interval == -1) {
        ereport(ERROR, errmsg("invalid chunk interval for hypertable %d", hypertable_id));
    }
    chunk_start = chunk_calculate_start(timestamp, chunk_interval);

    // search inside cache
    cached_info = chunk_cache_search(hypertable_id, chunk_start);
    if(cached_info != NULL){
        ChunkInfo *result = (ChunkInfo *) palloc(sizeof(ChunkInfo));
        memcpy(result, cached_info, sizeof(ChunkInfo)); // save in cache
        return result;
    }

    // search inside database
    chunk_id = metadata_find_chunk(hypertable_id, timestamp);
    if(chunk_id != -1){
        ChunkInfo *info = chunk_get_info(chunk_id);
        chunk_cache_insert(hypertable_id, chunk_start, info);
        return info;
    }

    // elog(NOTICE, "create chunk");
    return chunk_create(hypertable_id, timestamp);
}