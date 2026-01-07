#include <postgres.h>
#include <fmgr.h>
#include <access/table.h>           
#include <access/htup_details.h>    
#include <catalog/namespace.h>       
#include <catalog/pg_type.h>         
#include <utils/builtins.h>          
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>

#include "metadata.h"
#include "trigger.h"

#define USECS_PER_DAY INT64CONST(86400000000)
#define USECS_PER_HOUR INT64CONST(3600000000)

/*
 * Convert interval to microsecond
 * Parameter: postgres Interval type
 * return microsecond
 */
static int64
interval_to_microseconds(Interval *interval)
{
    int64 result = 0;
    result += interval->day * USECS_PER_DAY;
    result += interval->time;

    return result;
}

/*
 * Find time column in table
 * Parameter:
 *      rel - Relation (table)
 *      time_column_name - column name
 * return Attribute Number (number of column)
 */
static AttrNumber
find_time_column(Relation rel, const char *time_column_name)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    AttrNumber time_attnum = InvalidAttrNumber;
    Form_pg_attribute time_attr = NULL;

    // find all column in table
    for(int i=0; i<tupdesc->natts;i ++){
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if(attr->attisdropped){
            continue;
        }
        if(strcmp(NameStr(attr->attname), time_column_name) == 0){
            time_attnum = attr->attnum;
            time_attr = attr;
            break;
        }
    }

    if(time_attnum == InvalidAttrNumber){
        ereport(ERROR, errmsg("column \"%s\" does not exist", time_column_name));
    }

    if(!time_attr->attnotnull){
        ereport(WARNING, (errmsg("time column \"%s\" should be NOT NULL", time_column_name)));
    }

    return time_attnum;
}

/*
 * Check is table validate for hypertable or not
 *
 */
static void
validate_table_for_hypertable(Relation rel)
{
    char relkind = rel->rd_rel->relkind;
    if(relkind != RELKIND_RELATION){
        const char *relkind_str;
        switch (relkind){
            case RELKIND_VIEW:
                relkind_str = "view";
                break;
            case RELKIND_MATVIEW:
                relkind_str = "materialized view";
                break;
            case RELKIND_FOREIGN_TABLE:
                relkind_str = "foreign table";
                break;
            case RELKIND_PARTITIONED_TABLE:
                relkind_str = "partitioned table";
                break;
            default:
                relkind_str = "non-table relation";
        }

        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is a %s, not a regular table",
                    RelationGetRelationName(rel), relkind_str)));
    }

    // check duplicate hypertable
    char *schema_name = get_namespace_name(RelationGetNamespace(rel));
    char *table_name = RelationGetRelationName(rel);
    if(metadata_is_hypertable(schema_name, table_name)){
        ereport(ERROR, errmsg("\"%s.%s\" is already a hypertable", schema_name, table_name));
    }
}

/*
 * Convert normal table to hypertable
 * 
 * create_hypertable(
 *      table_name REGCLASS,
 *      time_column_name NAME，
 *      chunk_time_interval INTERVAL
 * ) RETURNS VOID
 * 
 */
PG_FUNCTION_INFO_V1(create_hypertable);
Datum
create_hypertable(PG_FUNCTION_ARGS)
{
    Oid table_oid;
    text *time_column_text;
    char *time_column_name;
    Interval *chunk_interval;
    
    Relation rel;
    char *schema_name;
    char *table_name;
    AttrNumber time_attnum;
    Oid time_type;
    int64 interval_us;
    int hypertable_id;

    table_oid = PG_GETARG_OID(0);
    time_column_text = PG_GETARG_TEXT_PP(1);
    time_column_name = text_to_cstring(time_column_text);
    chunk_interval = PG_GETARG_INTERVAL_P(2);

    // check table 
    rel = table_open(table_oid, AccessExclusiveLock);
    schema_name = get_namespace_name(RelationGetNamespace(rel));
    table_name = pstrdup(RelationGetRelationName(rel));
    validate_table_for_hypertable(rel);

    // check time column
    time_attnum = find_time_column(rel, time_column_name);

    TupleDesc tupdesc = RelationGetDescr(rel);
    Form_pg_attribute time_attr = TupleDescAttr(tupdesc, time_attnum - 1);
    time_type = time_attr->atttypid;

    // convert from interval to microseconds
    interval_us = interval_to_microseconds(chunk_interval);
    if (interval_us >= USECS_PER_DAY)
    {
        elog(NOTICE, "Chunk time interval: %ld day(s)",
             interval_us / USECS_PER_DAY);
    }
    else if (interval_us >= USECS_PER_HOUR)
    {
        elog(NOTICE, "Chunk time interval: %ld hour(s)",
             interval_us / USECS_PER_HOUR);
    }
    else
    {
        elog(NOTICE, "Chunk time interval: %ld microseconds", interval_us);
    }

    // save metadata
    hypertable_id = metadata_insert_hypertable(schema_name, table_name);
    elog(NOTICE, "Created hypertable with ID: %d", hypertable_id);
    metadata_insert_dimension(hypertable_id,
                              time_column_name,
                              time_type,
                              interval_us);
    elog(NOTICE, "Added time dimension on column \"%s\"", time_column_name);

    trigger_create_on_hypertable(schema_name, table_name);

    // close table
    table_close(rel, AccessExclusiveLock);
    elog(NOTICE, "✅ Successfully converted \"%s.%s\" to hypertable", schema_name, table_name);
    
    PG_RETURN_VOID();
}

/*
 * Remove hypertable
 * 
 * drop_hypertable(
 *      table_name REGCLASS,
 * ) RETURNS VOID
 * 
 */
PG_FUNCTION_INFO_V1(drop_hypertable);
Datum
drop_hypertable(PG_FUNCTION_ARGS)
{
    Oid table_oid;
    Relation rel;
    char *schema_name;
    char *table_name;

    table_oid = PG_GETARG_OID(0);

    rel = table_open(table_oid, AccessExclusiveLock);
    schema_name = get_namespace_name(RelationGetNamespace(rel));
    table_name = pstrdup(RelationGetRelationName(rel)); 
    if(!metadata_is_hypertable(schema_name, table_name)){
        ereport(WARNING, (errmsg("\"%s.%s\" is not a hypertable", schema_name, table_name)));
    }

    metadata_drop_hypertable(schema_name, table_name);
    trigger_drop_on_hypertable(schema_name, table_name);

    table_close(rel, AccessExclusiveLock);
    elog(NOTICE, "✅ Successfully dropped hypertable \"%s.%s\"", schema_name, table_name);

    PG_RETURN_VOID();
}