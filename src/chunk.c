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
#include <executor/spi.h>

#include "metadata.h"
#include "chunk.h"

static int
chunk_get_next_number(int hypertable_id)
{
    StringInfoData query;
    int chunk_number = 1;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT COALESCE(COUNT(*), 0) + 1 FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id = %d",
        hypertable_id);
    
    
    int ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, errmsg("failed to get next chunk number"));
    }
    
    bool isnull;
    chunk_number = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    
    return chunk_number;
}

static ChunkInfo*
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
    Oid chunk_oid;

    initStringInfo(&query);
    appendStringInfo(&query,
        "CREATE TABLE %s.%s (LIKE %s.%s INCLUDING ALL) "
        "INHERITS (%s.%s)",
        chunk_schema, chunk_name, hypertable_schema, hypertable_name,
        hypertable_schema, hypertable_name);
    elog(DEBUG1, "Creating chunk table: %s", query.data);
    
    // create inherit table
    int ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        SPI_finish();
        ereport(ERROR, (errmsg("failed to create chunk table \"%s\"", chunk_name)));
    }

    chunk_oid = get_relname_relid(chunk_name, get_namespace_oid(chunk_schema, false));

    // add constraint
    resetStringInfo(&query);
    appendStringInfo(&query,
        "ALTER TABLE %s.%s "
        "ADD CONSTRAINT %s_time_check "
        "CHECK (%s >= '2000-01-01 UTC'::timestamptz + '%ld microseconds'::interval "
        "AND %s < '2000-01-01 UTC'::timestamptz + '%ld microseconds'::interval)",
        chunk_schema, chunk_name,
        chunk_name,
        time_column, start_time,
        time_column, end_time);

    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        SPI_finish();
        ereport(ERROR, errmsg("failed to add time constraint to chunk \"%s\"", chunk_name));
    }

    return chunk_oid;
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

    chunk_id = metadata_insert_chunk(hypertable_id,
                                    hypertable_schema,
                                    chunk_name,
                                    chunk_start,
                                    chunk_end);
    
    ChunkInfo *info = (ChunkInfo *) palloc(sizeof(ChunkInfo));

    info->chunk_id = chunk_id;
    strcpy(info->schema_name, hypertable_schema);
    strcpy(info->table_name, chunk_name);
    info->start_time = chunk_start;
    info->end_time = chunk_end;
    
    elog(NOTICE, "✅ Chunk %d created successfully (OID: %u)", info->chunk_id, chunk_oid);
    return info;
}


ChunkInfo* 
chunk_get_or_create(int hypertable_id, int64 timestamp)
{
    int chunk_id;
    chunk_id = metadata_find_chunk(hypertable_id, timestamp);

    if(chunk_id != -1){
        return chunk_get_info(chunk_id);
    }

    return chunk_create(hypertable_id, timestamp);
}


PG_FUNCTION_INFO_V1(test_create_chunk);
Datum
test_create_chunk(PG_FUNCTION_ARGS)
{
    int hypertable_id = PG_GETARG_INT32(0);
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
    int64 time_us = timestamp;
    
    SPI_connect();
    int chunk_id = chunk_create(hypertable_id, time_us)->chunk_id;
    SPI_finish();
    
    PG_RETURN_INT32(chunk_id);
}


PG_FUNCTION_INFO_V1(test_find_chunk_for_time);
Datum
test_find_chunk_for_time(PG_FUNCTION_ARGS)
{
    int hypertable_id = PG_GETARG_INT32(0);
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
    
    int64 time_us = timestamp;
    SPI_connect();
    int chunk_id = metadata_find_chunk(hypertable_id, time_us);
    SPI_finish();

    PG_RETURN_INT32(chunk_id);
}

PG_FUNCTION_INFO_V1(test_get_or_create_chunk);
Datum
test_get_or_create_chunk(PG_FUNCTION_ARGS)
{
    int hypertable_id = PG_GETARG_INT32(0);
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
    int64 time_us = timestamp;

    SPI_connect();
    int chunk_id = chunk_get_or_create(hypertable_id, time_us)->chunk_id;
    SPI_finish();

    PG_RETURN_INT32(chunk_id);
}