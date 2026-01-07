#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <commands/trigger.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <access/htup_details.h>


#include "metadata.h"
#include "chunk.h"

/*
 * ==========================================
 * Private
 * ==========================================
 */

// Get time stamp from tuple
static int64
get_time_value_from_tuple(HeapTuple tuple, TupleDesc tupdesc, AttrNumber time_attnum)
{
    bool isnull;
    Datum time_datum;
    TimestampTz timestamp;

    time_datum = heap_getattr(tuple, time_attnum, tupdesc, &isnull);
    if(isnull){
        ereport(ERROR, errmsg("time column cannot be NULL"));
    }
    timestamp = DatumGetTimestampTz(time_datum);

    return timestamp;
}

// Create chunk table name
static char* 
get_chunk_table_name(const char *schema_name, const char *table_name)
{   
    StringInfoData name;
    initStringInfo(&name);
    
    appendStringInfo(&name, "%s.%s", 
                     quote_identifier(schema_name),
                     quote_identifier(table_name));
    
    return name.data;
}

/*
 * Get chunk info by chunk id
 * Parameters:
 *   chunk_id
 *   schema_name_out: output of chunk's schema
 *   table_name_out: output of chunk table name
 */
static void
get_chunk_info(int chunk_id, char **schema_name_out, char **table_name_out)
{
    StringInfoData query;
    int ret;
    bool isnull;
    Datum datum;

    initStringInfo(&query);
    appendStringInfo(&query,
                    "SELECT schema_name, table_name "
                    "FROM _timeseries_catalog.chunk "
                    "WHERE id = %d", chunk_id);
    
    
    ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        ereport(ERROR, errmsg("chunk with id %d not found", chunk_id));
    }

    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    *schema_name_out = pstrdup(TextDatumGetCString(datum));
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
    *table_name_out = pstrdup(TextDatumGetCString(datum));
}

/*
 * Build INSERT statement for chunk
 * Parameters:
 *   chunk_table - chunk table name
 *   tupdesc - Tuple descriptor
 *   tuple - Data want to insert
 *
 * Returns:
 *   SQL statement
 */
static char*
build_insert_query(const char *chunk_table, TupleDesc tupdesc, HeapTuple tuple)
{
    StringInfoData query;
    StringInfoData values;
    int num_atts = tupdesc->natts;
    
    initStringInfo(&query);
    initStringInfo(&values);

    appendStringInfo(&query, "INSERT INTO %s VALUES (", chunk_table);

    for(int i=0; i<num_atts; i++){
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        bool isnull;
        Datum value;

        if (attr->attisdropped) continue;

        value = heap_getattr(tuple, attr->attnum, tupdesc, &isnull);

        if(i>0){
            appendStringInfoString(&query, ", ");
        }
        if(isnull){
            appendStringInfoString(&query, "NULL");
        } else {
            Oid typoutput;
            bool typIsVarlena;
            char *value_str;

            getTypeOutputInfo(attr->atttypid, &typoutput, &typIsVarlena);
            value_str = OidOutputFunctionCall(typoutput, value);
            
            switch(attr->atttypid){
                case TEXTOID:
                case VARCHAROID:
                case BPCHAROID:
                case TIMESTAMPOID:
                case TIMESTAMPTZOID:
                case DATEOID:
                case TIMEOID:
                case TIMETZOID:
                    appendStringInfo(&query, "%s", quote_literal_cstr(value_str));
                    break;
                default:
                    appendStringInfo(&query, "%s", value_str);
                    break;
            }
        }
    }
    appendStringInfoString(&query, ")");

    return query.data;
}

/*
 * Hypertable insert trigger
 * Parameters:
 *   chunk_table - chunk table name
 *   tupdesc - Tuple descriptor
 *   tuple - Data want to insert
 *
 * Returns:
 *   SQL statement
 */
PG_FUNCTION_INFO_V1(hypertable_insert_trigger);
Datum
hypertable_insert_trigger(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    Relation rel;
    char *schema_name;
    char *table_name;
    int hypertable_id;
    AttrNumber time_attnum;
    int64 time_value;
    int chunk_id;
    char *chunk_schema;
    char *chunk_table;
    char *chunk_full_name;
    char *insert_query;
    int ret;

    // check trigger
    if(!CALLED_AS_TRIGGER(fcinfo)){
        ereport(ERROR, errmsg("hypertable_insert_trigger: not called by trigger manager"));
    }

    if(!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)){
        ereport(ERROR, errmsg("hypertable_insert_trigger: can only be used for INSERT"));
    }

    if(!TRIGGER_FIRED_BEFORE(trigdata->tg_event)){
        ereport(ERROR, errmsg("hypertable_insert_trigger: must be a BEFORE trigger"));
    }

        // check trigger is fired for wach row
    if(!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)){
        ereport(ERROR, errmsg("hypertable_insert_trigger: must be a FOR EACH ROW trigger"));
    }

    // fetch hypertable
    rel = trigdata->tg_relation;
    schema_name = get_namespace_name(RelationGetNamespace(rel));
    table_name = RelationGetRelationName(rel);

    hypertable_id = metadata_get_hypertable_id(schema_name, table_name);
    if(hypertable_id == -1){
        ereport(ERROR, errmsg("table \"%s.%s\" is not a hypertable", schema_name, table_name));
    }

    // find time column
    StringInfoData query;
    bool isnull;
    Datum datum;
    char *time_column_name;

    
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT column_name FROM _timeseries_catalog.dimension "
        "WHERE hypertable_id = %d", hypertable_id);
        
    SPI_connect();
    ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, errmsg("no dimension found for hypertable %d", hypertable_id));
    }

    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    time_column_name = pstrdup(TextDatumGetCString(datum));
    
    // find attribute number of time column
    TupleDesc tupdesc = RelationGetDescr(rel);
    time_attnum = InvalidAttrNumber;
    for(int i=0; i<tupdesc->natts; i++){
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        
        if (attr->attisdropped){
            continue;
        }
        
        if (strcmp(NameStr(attr->attname), time_column_name) == 0){
            time_attnum = attr->attnum;
            break;
        }
    }
    if (time_attnum == InvalidAttrNumber){
        ereport(ERROR, errmsg("time column \"%s\" not found", time_column_name));
    }
    
    // fetch timestamp
    time_value = get_time_value_from_tuple(trigdata->tg_trigtuple, tupdesc, time_attnum);
    
    SPI_finish();

    chunk_id = chunk_get_or_create(hypertable_id, time_value);
    elog(DEBUG1, "Using chunk_id: %d", chunk_id);

    SPI_connect();

    // fetch chunk
    get_chunk_info(chunk_id, &chunk_schema, &chunk_table);
    chunk_full_name = get_chunk_table_name(chunk_schema, chunk_table);
    elog(DEBUG1, "Target chunk: %s", chunk_full_name);

    // execute insert
    insert_query = build_insert_query(chunk_full_name, tupdesc, trigdata->tg_trigtuple);
    elog(DEBUG1, "executing INSERT query...");

    ret = SPI_execute(insert_query, false, 0);
    if (ret != SPI_OK_INSERT){
        SPI_finish();
        ereport(ERROR, errmsg("failed to insert into chunk \"%s\"", chunk_full_name));
    }
    SPI_finish();
    
    PG_RETURN_NULL();  // return NULL จะยกเลิก INSERT ใน table ที่ trigger อยู่ since it already inserted chunk
}

/*
 * ==========================================
 * Public functions
 * ==========================================
 */

// create trigger
void
trigger_create_on_hypertable(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    char trigger_name[NAMEDATALEN];

    snprintf(trigger_name, NAMEDATALEN, "ts_insert_trigger");

    initStringInfo(&query);
    appendStringInfo(&query,
                    "CREATE TRIGGER %s "
                        "BEFORE INSERT ON %s.%s "
                        "FOR EACH ROW "
                    "EXECUTE FUNCTION hypertable_insert_trigger()",
                    trigger_name,
                    quote_identifier(schema_name), quote_identifier(table_name));
    
    elog(DEBUG1, "Creating trigger: %s", query.data);
    
    SPI_connect();

    int ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        SPI_finish();
        ereport(ERROR, errmsg("failed to create insert trigger on \"%s.%s\"",
                        schema_name, table_name));
    }

    SPI_finish();
    elog(NOTICE, "Created INSERT trigger on \"%s.%s\"", schema_name, table_name);
}

// drop trigger
void 
trigger_drop_on_hypertable(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    char trigger_name[NAMEDATALEN];

    snprintf(trigger_name, NAMEDATALEN, "ts_insert_trigger");

    initStringInfo(&query);
    appendStringInfo(&query,
                    "DROP TRIGGER IF EXISTS %s ON %s.%s",
                    trigger_name, quote_identifier(schema_name), quote_identifier(table_name));
    
    SPI_connect();
    
    int ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY){
        SPI_finish();
        ereport(WARNING, errmsg("failed to drop insert trigger on \"%s.%s\"", schema_name, table_name));
        return;
    }
    
    SPI_finish();
    
    elog(NOTICE, "Dropped INSERT trigger from \"%s.%s\"", schema_name, table_name);
}

