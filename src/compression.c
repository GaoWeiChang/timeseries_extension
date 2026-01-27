#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/namespace.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <access/htup_details.h>
#include <funcapi.h>

#include "metadata.h"
#include "chunk.h"

/*
* Private function
*/
static bool 
chunk_is_compressed(int chunk_id)
{
    StringInfoData query;
    bool is_compressed = false;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT compressed "
        "FROM _timeseries_catalog.chunk "
        "WHERE id = %d", chunk_id);
    
    SPI_connect();
    int ret = SPI_execute(query.data, true, 0);
    
    if (ret == SPI_OK_SELECT && SPI_processed > 0){
        bool isnull;
        Datum compressed_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull){
            is_compressed = DatumGetBool(compressed_datum);
        }
    }
    SPI_finish();
    
    return is_compressed;
}

static void
mark_chunk_compressed(int chunk_id, bool compressed)
{
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query, 
        "UPDATE _timeseries_catalog.chunk "
        "SET compressed = %s, compressed_at = %s "
        "WHERE id = %d",
        compressed ? "true":"false", compressed ? "NOW()":"NULL",
        chunk_id);
    
    SPI_connect();
    int ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UPDATE){
        SPI_finish();
        ereport(ERROR, errmsg("failed to update compression status for chunk %d", chunk_id));
    }
    SPI_finish();
}

static int64
get_table_size(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    int64 size_byte = 0;

    SPI_connect();
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT pg_total_relation_size('%s.%s')",
        quote_identifier(schema_name), quote_identifier(table_name));
    
    int ret = SPI_execute(query.data, true, 0);
    if(ret == SPI_OK_SELECT && SPI_processed > 0){
        bool isnull;
        size_byte = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    }    
    SPI_finish();

    return size_byte;
}

static void
compress_chunk_data(const char *schema_name, const char *table_name)
{
    StringInfoData query;
    char compressed_table[NAMEDATALEN];

    // build compressed table
    snprintf(compressed_table, NAMEDATALEN, "%s_compressed", table_name);

    initStringInfo(&query);
    appendStringInfo(&query,
        "CREATE TABLE %s.%s (LIKE %s.%s INCLUDING ALL) "
        "WITH (toast_tuple_target = 128)",
        quote_identifier(schema_name), compressed_table, 
        quote_identifier(schema_name), quote_identifier(table_name));
    
    SPI_connect();
    int ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        SPI_finish();
        ereport(ERROR, errmsg("failed to create compressed table for %s", table_name));
    }

    // copy & compressed data to compressed table 
    resetStringInfo(&query);
    appendStringInfo(&query,
        "INSERT INTO %s.%s SELECT * FROM %s.%s",
        quote_identifier(schema_name), compressed_table, 
        quote_identifier(schema_name), quote_identifier(table_name));
    
    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_INSERT){
        SPI_finish();
        ereport(ERROR, errmsg("failed to copy data to compressed table"));
    }

    // reclaim disk space
    resetStringInfo(&query);
    appendStringInfo(&query,
        "ANALYZE %s.%s",
        quote_identifier(schema_name), compressed_table);

    ret = SPI_execute(query.data, false, 0);

    // drop original table
    resetStringInfo(&query);
    appendStringInfo(&query,
        "DROP TABLE %s.%s",
        quote_identifier(schema_name), quote_identifier(table_name));

    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        SPI_finish();
        ereport(ERROR, errmsg("failed to drop original chunk table"));
    }

    // rename compressed table 
    resetStringInfo(&query);
    appendStringInfo(&query,
        "ALTER TABLE %s.%s RENAME TO %s",
        quote_identifier(schema_name), compressed_table,
        quote_identifier(table_name));
    
    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UTILITY){
        SPI_finish();
        ereport(ERROR, errmsg("failed to rename compressed table"));
    }

    SPI_finish();
}

/*
* Top level function
*/
PG_FUNCTION_INFO_V1(compress_chunk);
Datum 
compress_chunk(PG_FUNCTION_ARGS)
{
    int chunk_id = PG_GETARG_INT32(0);
    ChunkInfo *chunk_info;
    int64 size_before, size_after;

    if(chunk_is_compressed(chunk_id)){
        ereport(NOTICE, errmsg("chunk %d is already compressed", chunk_id));
        PG_RETURN_BOOL(false);
    }

    SPI_connect();
    chunk_info = chunk_get_info(chunk_id);
    SPI_finish();

    if(!chunk_info){
        ereport(ERROR, errmsg("chunk with id %d not found", chunk_id));
    }

    size_before = get_table_size(chunk_info->schema_name, chunk_info->table_name);
    compress_chunk_data(chunk_info->schema_name, chunk_info->table_name); // compress
    size_after = get_table_size(chunk_info->schema_name, chunk_info->table_name);
    
    mark_chunk_compressed(chunk_id, true);
    
    elog(NOTICE, "✅ Chunk %d compressed", chunk_id);
    elog(NOTICE, "Size before: %ld bytes", size_before);
    elog(NOTICE, "Size after:  %ld bytes", size_after);
    
    PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(decompress_chunk);
Datum 
decompress_chunk(PG_FUNCTION_ARGS)
{
    int chunk_id = PG_GETARG_INT32(0);

    if(!chunk_is_compressed(chunk_id)){
        ereport(NOTICE,
            errmsg("chunk %d is already decompressed", chunk_id));
        PG_RETURN_BOOL(false);
    }

    mark_chunk_compressed(chunk_id, false);

    elog(NOTICE, "✅ Chunk %d decompressed", chunk_id);

    PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(compress_chunks_older_than);
Datum 
compress_chunks_older_than(PG_FUNCTION_ARGS)
{
    text *table_name_text = PG_GETARG_TEXT_PP(0);
    Interval *older_than = PG_GETARG_INTERVAL_P(1);

    char *table_name = text_to_cstring(table_name_text);
    char *schema_name = "public";
    int hypertable_id;
    StringInfoData query;
    int64 cutoff_time;
    int compressed_count = 0;

    SPI_connect();

    hypertable_id = metadata_get_hypertable_id(schema_name, table_name);
    if (hypertable_id == -1){
        SPI_finish();
        ereport(ERROR, errmsg("table \"%s\" is not a hypertable", table_name));
    }

    // calculate cutoff time
    TimestampTz now = GetCurrentTimestamp();
    cutoff_time = now - (older_than->time + older_than->day * USECS_PER_DAY);

    // find chunk that need to compress
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id "
        "FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id = %d "
        "AND end_time < " INT64_FORMAT " "
        "AND (compressed IS NULL OR compressed = false) "
        "ORDER BY start_time",
        hypertable_id, cutoff_time);

    int ret = SPI_execute(query.data, true, 0);
    if(ret == SPI_OK_SELECT && SPI_processed > 0){
        for(uint64 i=0; i<SPI_processed; i++){
            bool isnull;
            Datum chunk_id_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
            
            if(!isnull){
                // compress chunk
                int chunk_id = DatumGetInt32(chunk_id_datum);
                
                SPI_finish();
                DirectFunctionCall1(compress_chunk, Int32GetDatum(chunk_id));
                SPI_connect();
                
                compressed_count+=1;
            }
        }
    }

    SPI_finish();
    
    elog(NOTICE, "✅ Compressed %d chunk(s)", compressed_count);
    
    PG_RETURN_INT32(compressed_count);
}

PG_FUNCTION_INFO_V1(show_chunk_compression_stats);
Datum
show_chunk_compression_stats(PG_FUNCTION_ARGS)
{
    text *table_name_text = PG_GETARG_TEXT_PP(0);
    char *table_name = text_to_cstring(table_name_text);
    char *schema_name = "public";
    int hypertable_id;
    StringInfoData result;

    SPI_connect();

    hypertable_id = metadata_get_hypertable_id(schema_name, table_name);
    if (hypertable_id == -1){
        SPI_finish();
        ereport(ERROR, errmsg("table \"%s\" is not a hypertable", table_name));
    }

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT "
        "  COUNT(*) as total_chunks, "
        "  COUNT(*) FILTER (WHERE compressed = true) as compressed_chunks, "
        "  COUNT(*) FILTER (WHERE compressed = false OR compressed IS NULL) as uncompressed_chunks "
        "FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id = %d", hypertable_id);
    
    int ret = SPI_execute(query.data, true, 0);
    
    initStringInfo(&result);
    
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull;
        int total = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
        int compressed = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
        int uncompressed = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull));
        
        appendStringInfo(&result,
            "Compression Statistics for %s:\n"
            "  Total chunks:        %d\n"
            "  Compressed chunks:   %d (%.1f%%)\n"
            "  Uncompressed chunks: %d (%.1f%%)\n",
            table_name,
            total,
            compressed, total > 0 ? (compressed * 100.0 / total) : 0,
            uncompressed, total > 0 ? (uncompressed * 100.0 / total) : 0);
    }
    
    SPI_finish();
    
    PG_RETURN_TEXT_P(cstring_to_text(result.data));
}