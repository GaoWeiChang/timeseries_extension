#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <access/htup_details.h>
#include <funcapi.h>

#include "../../src/metadata.h"
#include "compression.h"

/*
CompressedChunkInfo* 
compress_chunk_internal(int chunk_id)
{
    StringInfoData query;
    char *schema_name, *table_name;
    int ret;
    bool isnull;
    CompressedChunkInfo *info;

    // get chunk
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT schema_name, table_name "
        "FROM _timeseries_catalog.chunk "
        "WHERE id = %d", chunk_id);
    
    ret = SPI_execute(query.data, true, 1);
    if(ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("chunk %d not found", chunk_id)));
    
    schema_name = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    table_name  = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

    if(schema_name == NULL || table_name == NULL)
        ereport(ERROR, (errmsg("chunk %d has NULL schema or table name", chunk_id)));

    // check chunk already compressed or not
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT 1 FROM _timeseries_catalog.chunk "
        "WHERE (id = %d) AND (is_compressed = TRUE)", chunk_id);

    ret = SPI_execute(query.data, true, 1);
    if(ret == SPI_OK_SELECT && SPI_processed > 0)
        ereport(ERROR, (errmsg("chunk %d is already compressed", chunk_id)));

    // get column info from chunk table
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = '%s' AND table_name = '%s' "
        "ORDER BY ordinal_position",
        schema_name, table_name);
    
    ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("failed to get column info for chunk %d", chunk_id)));
    
    // copy result to prevent SPI_tuptable overwrite
    int n_cols = (int) SPI_processed;
    typedef struct {
        char name[NAMEDATALEN];
        char type[NAMEDATALEN];
    } ColRow;
    
    ColRow *cols = (ColRow *) palloc(n_cols * sizeof(ColRow));

    for(int i=0; i<n_cols; i++){
        char *val;
        val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
        if(val == NULL)
            ereport(ERROR, (errmsg("NULL column name at index %d", i)));
        strlcpy(cols[i].name, val, NAMEDATALEN);

        val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
        if(val == NULL)
            ereport(ERROR, (errmsg("NULL column type at index %d", i)));
        strlcpy(cols[i].type, val, NAMEDATALEN);
    }

    // count row
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT COUNT(*) FROM %s.%s",
        quote_identifier(schema_name), quote_identifier(table_name));
    
    ret = SPI_execute(query.data, true, 1);
    int64 row_count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    if(row_count == 0){
        elog(NOTICE, "chunk %d is empty, skipping compression", chunk_id);
        return NULL;
    }

    // get uncompresed size
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT pg_total_relation_size(%s)",
        quote_literal_cstr(psprintf("%s.%s", schema_name, table_name)));

    ret = SPI_execute(query.data, true, 1);
    if(ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("failed to get size of chunk %d", chunk_id)));

    int64 uncompressed_bytes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    
    // make each column into array and insert compressed data into comrpessed table
    for(int i=0; i<n_cols; i++){
        resetStringInfo(&query);
        appendStringInfo(&query,
            "INSERT INTO _timeseries_catalog.compressed_chunk "
            "    (chunk_id, column_name, column_type, column_data) "
            "SELECT %d, '%s', '%s', string_agg(%s::text, '|')::bytea "
            "FROM %s.%s",
            chunk_id, cols[i].name, cols[i].type, quote_identifier(cols[i].name), 
            quote_identifier(schema_name), quote_identifier(table_name));
        
        ret = SPI_execute(query.data, false, 0);
        if (ret != SPI_OK_INSERT)
            ereport(ERROR, (errmsg("failed to compress column %s", cols[i].name)));
        
        elog(NOTICE, "compressed column %s (%s)", cols[i].name, cols[i].type);
    }

    // update row count
    resetStringInfo(&query);
    appendStringInfo(&query,
        "UPDATE _timeseries_catalog.compressed_chunk "
        "SET row_count = " INT64_FORMAT ", uncompressed_bytes = " INT64_FORMAT " "
        "WHERE chunk_id = %d",
        row_count, uncompressed_bytes, chunk_id);
    
    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UPDATE)
        ereport(WARNING, (errmsg("failed to update metadata for chunk %d", chunk_id)));
    
    // get compressed size
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT COALESCE(SUM(pg_column_size(column_data)), 0) "
        "FROM _timeseries_catalog.compressed_chunk "
        "WHERE chunk_id = %d", chunk_id);

    SPI_execute(query.data, true, 1);

    int64 compressed_bytes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    
    // update compress flag
    resetStringInfo(&query);
    appendStringInfo(&query,
        "UPDATE _timeseries_catalog.chunk "
        "SET is_compressed = TRUE "
        "WHERE id = %d", chunk_id);
    
    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UPDATE)
        ereport(ERROR, (errmsg("failed to mark chunk %d as compressed", chunk_id)));
    
    // drop original chunk
    resetStringInfo(&query);
    appendStringInfo(&query,
        "DROP TABLE %s.%s",
        quote_identifier(schema_name), quote_identifier(table_name));

    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY)
        ereport(WARNING, (errmsg("failed to drop original chunk table %s.%s", schema_name, table_name)));


    info = (CompressedChunkInfo *) palloc(sizeof(CompressedChunkInfo));
    info->chunk_id = chunk_id;
    info->original_row_count = row_count;
    info->uncompressed_bytes = uncompressed_bytes;
    info->compressed_bytes = compressed_bytes;
    info->compression_ratio  = (compressed_bytes > 0) ? ((double) uncompressed_bytes / (double) compressed_bytes) : 0.0;
    info->is_compressed = true;

    elog(NOTICE, "chunk %d compressed: %ld rows, %.2f MB → %.2f MB (compression ratio: %.2f)",
        chunk_id, row_count,
        uncompressed_bytes / (1024.0 * 1024.0),
        compressed_bytes / (1024.0 * 1024.0),
        info->compression_ratio);

    return info;
}   
*/


CompressedChunkInfo* 
compress_chunk_internal(int chunk_id)
{
    StringInfoData query;
    char *schema_name, *table_name;
    int ret;
    bool isnull;
    CompressedChunkInfo *info;

    // get chunk
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT schema_name, table_name "
        "FROM _timeseries_catalog.chunk "
        "WHERE id = %d", chunk_id);
    
    ret = SPI_execute(query.data, true, 1);
    if(ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("chunk %d not found", chunk_id)));
    
    schema_name = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    table_name  = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

    if(schema_name == NULL || table_name == NULL)
        ereport(ERROR, (errmsg("chunk %d has NULL schema or table name", chunk_id)));

    // check chunk already compressed or not
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT 1 "
        "FROM _timeseries_catalog.compressed_chunk "
        "WHERE chunk_id = %d", chunk_id);

    ret = SPI_execute(query.data, true, 1);
    if(ret == SPI_OK_SELECT && SPI_processed > 0)
        ereport(ERROR, (errmsg("chunk %d is already compressed", chunk_id)));
    
    // get column info from chunk table
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = '%s' AND table_name = '%s' "
        "ORDER BY ordinal_position",
        schema_name, table_name);
    
    ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("failed to get column info for chunk %d", chunk_id)));
    
    // copy result to prevent SPI_tuptable overwrite
    int n_cols = (int) SPI_processed;
    typedef struct {
        char name[NAMEDATALEN];
        char type[NAMEDATALEN];
    } ColRow;
    
    ColRow *cols = (ColRow *) palloc(n_cols * sizeof(ColRow));

    for(int i=0; i<n_cols; i++){
        char *val;
        val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
        if(val == NULL)
            ereport(ERROR, (errmsg("NULL column name at index %d", i)));
        strlcpy(cols[i].name, val, NAMEDATALEN);

        val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
        if(val == NULL)
            ereport(ERROR, (errmsg("NULL column type at index %d", i)));
        strlcpy(cols[i].type, val, NAMEDATALEN);
    }

    // count row
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT COUNT(*) FROM %s.%s",
        quote_identifier(schema_name), quote_identifier(table_name));
    
    ret = SPI_execute(query.data, true, 1);
    int64 row_count = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    if(row_count == 0){
        elog(NOTICE, "chunk %d is empty, skipping compression", chunk_id);
        return NULL;
    }

    // get uncompresed size
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT pg_total_relation_size(%s)",
        quote_literal_cstr(psprintf("%s.%s", schema_name, table_name)));

    ret = SPI_execute(query.data, true, 1);
    if(ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("failed to get size of chunk %d", chunk_id)));

    int64 uncompressed_bytes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    
    // make each column into array and serialize
    for(int i=0; i<n_cols; i++){
        resetStringInfo(&query);
        appendStringInfo(&query,
            "INSERT INTO _timeseries_catalog.compressed_chunk "
            "    (chunk_id, column_name, column_type, column_data) "
            "SELECT %d, '%s', '%s', string_agg(%s::text, '|')::bytea "
            "FROM %s.%s",
            chunk_id, cols[i].name, cols[i].type, quote_identifier(cols[i].name), 
            quote_identifier(schema_name), quote_identifier(table_name));
        
        ret = SPI_execute(query.data, false, 0);
        if (ret != SPI_OK_INSERT)
            ereport(ERROR, (errmsg("failed to compress column %s", cols[i].name)));
        
        elog(NOTICE, "compressed column %s (%s)", cols[i].name, cols[i].type);
    }

    // update row count
    resetStringInfo(&query);
    appendStringInfo(&query,
        "UPDATE _timeseries_catalog.compressed_chunk "
        "SET row_count = " INT64_FORMAT ", uncompressed_bytes = " INT64_FORMAT " "
        "WHERE chunk_id = %d",
        row_count, uncompressed_bytes, chunk_id);
    
    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UPDATE)
        ereport(WARNING, (errmsg("failed to update metadata for chunk %d", chunk_id)));

    // drop original chunk
    resetStringInfo(&query);
    appendStringInfo(&query,
        "DROP TABLE %s.%s",
        quote_identifier(schema_name), quote_identifier(table_name));

    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY)
        ereport(WARNING, (errmsg("failed to drop original chunk table %s.%s", schema_name, table_name)));
    
    // get compressed size
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT COALESCE(SUM(pg_column_size(column_data)), 0) "
        "FROM _timeseries_catalog.compressed_chunk "
        "WHERE chunk_id = %d", chunk_id);

    SPI_execute(query.data, true, 1);

    int64 compressed_bytes = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

    info = (CompressedChunkInfo *) palloc(sizeof(CompressedChunkInfo));
    info->chunk_id = chunk_id;
    info->original_row_count = row_count;
    info->uncompressed_bytes = uncompressed_bytes;
    info->compressed_bytes = compressed_bytes;
    info->compression_ratio  = (compressed_bytes > 0) ? ((double) uncompressed_bytes / (double) compressed_bytes) : 0.0;
    info->is_compressed = true;

    elog(NOTICE, "chunk %d compressed: %ld rows, %.2f MB → %.2f MB (compression ratio: %.2f)",
        chunk_id, row_count,
        uncompressed_bytes / (1024.0 * 1024.0),
        compressed_bytes / (1024.0 * 1024.0),
        info->compression_ratio);

    return info;
}   


PG_FUNCTION_INFO_V1(compress_chunk);
Datum 
compress_chunk(PG_FUNCTION_ARGS)
{
    Oid chunk_oid = PG_GETARG_OID(0);
    StringInfoData query;
    char *schema_name = get_namespace_name(get_rel_namespace(chunk_oid));
    char *table_name = get_rel_name(chunk_oid);
    int ret;
    int chunk_id;
    bool isnull;
    CompressedChunkInfo *info;

    SPI_connect();

    // get chunk id
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id "
        "FROM _timeseries_catalog.chunk "
        "WHERE schema_name = '%s' AND table_name = '%s'",
        schema_name, table_name);
    
    ret = SPI_execute(query.data, true, 1);
    if(ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, (errmsg("table %s.%s is not a chunk", schema_name, table_name)));
    }

    chunk_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

    // compress
    info = compress_chunk_internal(chunk_id);

    SPI_finish();
    PG_RETURN_VOID();
}