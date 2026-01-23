#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <catalog/namespace.h>

#include "metadata.h"

bool 
metadata_is_hypertable(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT * FROM _timeseries_catalog.hypertable "
        "WHERE schema_name='%s' AND table_name='%s'",
        schema_name, table_name);
    
    SPI_execute(query.data, true, 0);
    bool exists = (SPI_processed > 0);

    return exists;
}

int 
metadata_insert_hypertable(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    int hypertable_id;
    bool isnull;

    initStringInfo(&query);
    appendStringInfo(&query,
        "INSERT INTO _timeseries_catalog.hypertable (schema_name, table_name) "
        "VALUES (%s, %s) RETURNING id",
        quote_literal_cstr(schema_name), quote_literal_cstr(table_name));
    
    SPI_execute(query.data, false, 0);
    if (SPI_processed <= 0){
        ereport(ERROR, 
                (errmsg("failed to insert hypertable metadata")));
    }
    hypertable_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

    return hypertable_id;
}

int 
metadata_get_hypertable_id(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    int hypertable_id = -1;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id FROM _timeseries_catalog.hypertable "
        "WHERE schema_name='%s' AND table_name='%s'",
        schema_name, table_name);
    
    SPI_execute(query.data, true, 0);
    if (SPI_processed > 0){
        bool isnull;
        hypertable_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    }

    return hypertable_id;
}

void 
metadata_drop_hypertable(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "DELETE FROM _timeseries_catalog.hypertable "
        "WHERE schema_name=%s AND table_name=%s",
        quote_literal_cstr(schema_name), quote_literal_cstr(table_name));
    
    int ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_DELETE){
        ereport(ERROR, 
                (errcode(ERRCODE_INTERNAL_ERROR),errmsg("Failed to delete hypertable %s", table_name)));
    }
}

void 
metadata_insert_dimension(int hypertable_id,
                          const char *column_name,
                          Oid column_type,
                          int64 interval_microseconds)
{
    StringInfoData query;
    char *type_name = format_type_be(column_type);

    initStringInfo(&query);
    appendStringInfo(&query,
                    "INSERT INTO _timeseries_catalog.dimension "
                    "(hypertable_id, column_name, column_type, interval_length) "
                    "VALUES (%d, '%s', '%s', " INT64_FORMAT ")",
                    hypertable_id, column_name, type_name, interval_microseconds);
    
    SPI_execute(query.data, false, 0);
    if (SPI_processed <= 0){
        ereport(ERROR, 
                (errmsg("failed to insert dimension metadata")));
    }
}

int64
metadata_get_chunk_interval(int hypertable_id)
{
    StringInfoData query;
    int64 interval = -1;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT interval_length FROM _timeseries_catalog.dimension "
        "WHERE hypertable_id=%d",
        hypertable_id);
    
    SPI_execute(query.data, true, 0);
    if (SPI_processed > 0){
        bool isnull;
        interval = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    }
    
    return interval;
}

int 
metadata_insert_chunk(int hypertable_id,
                          const char *schema_name,
                          const char *table_name,
                          int64 start_time,
                          int64 end_time)
{
    StringInfoData query;
    int chunk_id;
    bool isnull;

    initStringInfo(&query);
    appendStringInfo(&query,
        "INSERT INTO _timeseries_catalog.chunk "
        "(hypertable_id, schema_name, table_name, start_time, end_time) "
        "VALUES (%d, '%s', '%s', " INT64_FORMAT ", " INT64_FORMAT ") RETURNING id",
        hypertable_id, schema_name, table_name, start_time, end_time);
    
    SPI_execute(query.data, false, 0);
    if (SPI_processed <= 0){
        ereport(ERROR,
                (errmsg("failed to insert dimension metadata")));
    }
    chunk_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

    return chunk_id;
}

int 
metadata_find_chunk(int hypertable_id, int64 time_microseconds)
{
    StringInfoData query;
    int chunk_id = -1;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id=%d AND start_time<=" INT64_FORMAT " AND end_time>" INT64_FORMAT,
        hypertable_id, time_microseconds, time_microseconds);
    
    SPI_execute(query.data, true, 0);
    if (SPI_processed > 0){
        bool isnull;
        chunk_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    }

    return chunk_id;
}
