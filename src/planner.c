#include <postgres.h>
#include <optimizer/planner.h>
#include <optimizer/pathnode.h>
#include <nodes/pg_list.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <utils/builtins.h>
#include <executor/spi.h>
#include <nodes/makefuncs.h>
#include <parser/parsetree.h>

#include "metadata.h"

static planner_hook_type prev_planner_hook = NULL;

static bool 
is_hypertable_relation(RangeTblEntry *rte)
{
    char *schema_name;
    char *table_name;
    bool result = false;

    if(rte->rtekind != RTE_RELATION){
        return false;
    }

    schema_name = get_namespace_name(get_rel_namespace(rte->relid));
    table_name = get_rel_name(rte->relid);

    // avoid infinite recursion when planner check _timeseries_catalog
    if(strcmp(schema_name, "_timeseries_catalog") == 0){
        return false;
    }

    // check schema is dropped with extension
    Oid catalog_schema_oid = get_namespace_oid("_timeseries_catalog", true);
    if (catalog_schema_oid == InvalidOid){
        return false; 
    }

    SPI_connect();
    result = metadata_is_hypertable(schema_name, table_name);
    SPI_finish();

    return result;
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

    if(prev_planner_hook){
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
    prev_planner_hook = planner_hook;
    planner_hook = timeseries_planner_hook;
    elog(LOG, "Timeseries planner hook installed");
}

void
planner_hook_cleanup(void)
{
    planner_hook = prev_planner_hook;
    elog(LOG, "Timeseries planner hook removed");
}
