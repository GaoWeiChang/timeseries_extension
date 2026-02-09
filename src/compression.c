#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <access/htup_details.h>
#include <funcapi.h>
#include <string.h>

#include "dictionary.h"
#include "delta.h"
#include "delta_of_delta.h"

#define ALG_DICTIONARY "dictionary"
#define ALG_DELTA "delta"
#define ALG_DELTA_OF_DELTA "delta_of_delta"
#define ALG_NONE "NONE"


/*
* Private function
*/
static bool 
chunk_is_compressed(int chunk_id)
{
    StringInfoData query;
    bool is_compressed = false;
    int ret;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT compressed "
        "FROM _timeseries_catalog.chunk "
        "WHERE id = %d", chunk_id);
    
    SPI_connect();
    ret = SPI_execute(query.data, true, 0);
    
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
    int ret;

    initStringInfo(&query);
    appendStringInfo(&query, 
        "UPDATE _timeseries_catalog.chunk "
        "SET compressed = %s, compressed_at = %s "
        "WHERE id = %d",
        compressed ? "true":"false", compressed ? "NOW()":"NULL",
        chunk_id);
    
    SPI_connect();
    ret = SPI_execute(query.data, false, 0);
    if(ret != SPI_OK_UPDATE){
        SPI_finish();
        ereport(ERROR, errmsg("failed to mark compression status for chunk %d", chunk_id));
    }
    SPI_finish();
}


static bool
get_chunk_table(int chunk_id, char *schema_out, char *table_out)
{
    int ret;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT schema_name, table_name "
        "FROM _timeseries_catalog.chunk "
        "WHERE id = %d", chunk_id);
    
    SPI_connect();
    ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0){ 
        SPI_finish(); 
        return false; 
    }

    bool isnull;
    strncpy(schema_out, TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull)), NAMEDATALEN);
    strncpy(table_out,  TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull)), NAMEDATALEN);

    SPI_finish();
    return true;
}

// create compressed_column metadata table
static void
ensure_compressed_columns_table(void)
{
    SPI_connect();
    SPI_execute(
        "CREATE TABLE IF NOT EXISTS _timeseries_catalog.compressed_columns ("
        "  chunk_id        INTEGER     NOT NULL REFERENCES _timeseries_catalog.chunk(id),"
        "  column_name     TEXT        NOT NULL,"
        "  column_type     OID         NOT NULL,"
        "  algorithm       TEXT        NOT NULL,"
        "  compressed_data BYTEA       NOT NULL,"
        "  PRIMARY KEY (chunk_id, column_name)"
        ")",
        false, 0);
    SPI_finish();
}

/*
* Top level function
*/

/*
Compression step: 
  - compress each column based on data type
  - convert compressed data to  bytea for store in table
  - store compressed data into database via payload
  **payload: vehicle for storing compressed data into the database
*/
PG_FUNCTION_INFO_V1(compress_chunk);
Datum 
compress_chunk(PG_FUNCTION_ARGS)
{
    int chunk_id = PG_GETARG_INT32(0);
    char schema[NAMEDATALEN], table[NAMEDATALEN];
    StringInfoData query;

    if(chunk_is_compressed(chunk_id)){
        ereport(NOTICE, errmsg("chunk %d is already compressed", chunk_id));
        PG_RETURN_BOOL(false);
    }

    if(!get_chunk_table(chunk_id, schema, table)){
        ereport(ERROR, (errmsg("chunk %d not found", chunk_id)));
    }

    ensure_compressed_columns_table();
    elog(NOTICE, "Compressing chunk %d (%s.%s)…", chunk_id, schema, table);

    // read column
    SPI_connect();
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT attname, atttypid FROM pg_attribute "
        "WHERE attrelid = '%s.%s'::regclass AND attnum > 0 AND NOT attisdropped "
        "ORDER BY attnum",
        schema, table);

    SPI_execute(query.data, true, 0);
    
    // store column info
    int32 n_cols = (int32) SPI_processed;
    char (*col_names)[NAMEDATALEN] = palloc(n_cols * sizeof(char[NAMEDATALEN]));
    Oid *col_types = palloc(n_cols * sizeof(Oid));
    for(int32 i=0; i<n_cols; i++){
        bool isnull;
        strncpy(col_names[i], 
                TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull)), 
                NAMEDATALEN);
        col_types[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
    }
    SPI_finish();

    // compress each column and serialise
    for(int32 col=0; col<n_cols; col++){
        Oid type = col_types[col];
        char *col_name = col_names[col];
        const char *algo = ALG_NONE; 
        bytea *payload = NULL;

        SPI_connect();
        initStringInfo(&query);
        appendStringInfo(&query, "SELECT %s FROM %s.%s ORDER BY ctid",
            quote_identifier(col_name), quote_identifier(schema), quote_identifier(table));
        SPI_execute(query.data, true, 0);

        int32 num_rows = (int32) SPI_processed;

        // use compression algorithm based on column type
        if (type == TIMESTAMPTZOID || type == TIMESTAMPOID){
            algo = ALG_DELTA_OF_DELTA;
            int64 *timestamps = (int64 *) palloc(num_rows * sizeof(int64));
            for(int32 i=0; i<num_rows; i++){
                bool isnull;
                Datum datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
                timestamps[i] = isnull ? 0 : DatumGetInt64(datum);
            }
            SPI_finish();
            
            DodCompressed *compressed = compress_timestamp_column_with_dod(col_name, timestamps, num_rows);
            payload = dod_serialise(compressed);
        }
        else if (type == INT4OID || type == INT8OID || type == INT2OID){
            algo = ALG_DELTA;
            int64 *vals = (int64 *) palloc(num_rows * sizeof(int64));
            for (int32 i = 0; i < num_rows; i++){
                bool isnull;
                Datum datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
                if (isnull) vals[i] = 0;
                else if (type == INT2OID) vals[i] = (int64) DatumGetInt16(datum);
                else if (type == INT4OID) vals[i] = (int64) DatumGetInt32(datum);
                else vals[i] = DatumGetInt64(datum);
            }
            SPI_finish();

            DeltaCompressed *compressed = compress_int_column_with_delta(col_name, type, vals, num_rows);
            payload = delta_serialise(compressed);
        }
        else if (type == TEXTOID || type == VARCHAROID || type == NAMEOID){
            algo = ALG_DICTIONARY;
            text **vals = (text **) palloc(num_rows * sizeof(text *));
            for(int32 i=0; i<num_rows; i++){
                bool isnull;
                Datum datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
                vals[i] = isnull ? cstring_to_text("") : DatumGetTextPP(datum); 
            }
            SPI_finish();

            DictCompressed *compressed = compress_text_column_with_dictionary(col_name, vals, num_rows);
            payload = dict_serialise(compressed);   
        }
        else{
            algo = ALG_NONE;
            SPI_finish();
            elog(NOTICE, "  [None] %s: skipped (type %u)", col_name, type);
            continue;
        }

        // store payload in metadata
        SPI_connect();
        initStringInfo(&query);
        appendStringInfo(&query,
            "INSERT INTO _timeseries_catalog.compressed_columns "
            "(chunk_id, column_name, column_type, algorithm, compressed_data) "
            "VALUES (%d, '%s', %u, '%s', $1)",
            chunk_id, col_name, type, algo);
        
        {
            Oid paramTypes[1] = { BYTEAOID };
            Datum paramValues[1];
            char paramNulls[1] = { ' ' };
            paramValues[0] = PointerGetDatum(payload);

            SPI_execute_with_args(
                "INSERT INTO _timeseries_catalog.compressed_columns "
                "(chunk_id, column_name, column_type, algorithm, compressed_data) "
                "VALUES ($1, $2, $3, $4, $5)",
                5,
                (Oid[]) { INT4OID, TEXTOID, OIDOID, TEXTOID, BYTEAOID },
                (Datum[]) { Int32GetDatum(chunk_id),
                            CStringGetDatum(col_name),
                            ObjectIdGetDatum(type),
                            CStringGetDatum(algo),
                            PointerGetDatum(payload) },
                (char[])  { ' ', ' ', ' ', ' ', ' ' },
                false, 0);
        }
        SPI_finish();
    }

    // store column defs for decompress
    {
        StringInfoData defs;
        initStringInfo(&defs);
        appendStringInfo(&defs,
            "INSERT INTO _timeseries_catalog.compressed_columns "
            "(chunk_id, column_name, column_type, algorithm, compressed_data) "
            "VALUES (%d, '__table_def__', 0, 'meta', ('%s.%s')::bytea)",
            chunk_id, schema, table);
    }

    // drop original chunk table
    SPI_connect();
    initStringInfo(&query);
    appendStringInfo(&query, "DROP TABLE %s.%s", quote_identifier(schema), quote_identifier(table));
    SPI_execute(query.data, false, 0);
    SPI_finish();

    mark_chunk_compressed(chunk_id, true);
    elog(NOTICE, "✅ Chunk %d compressed successfully", chunk_id);

    PG_RETURN_BOOL(true);
}


/*
Decompression step
    - read compressed data
    - decompress
    - build table and store decompressed data
    - delete metadata
*/
PG_FUNCTION_INFO_V1(decompress_chunk);
Datum 
decompress_chunk(PG_FUNCTION_ARGS)
{
    int chunk_id = PG_GETARG_INT32(0);
    int32 n_cols = (int32) SPI_processed;
    char schema[NAMEDATALEN], table[NAMEDATALEN];
    StringInfoData query;

    if(!chunk_is_compressed(chunk_id)){
        ereport(NOTICE,
            errmsg("chunk %d is already decompressed", chunk_id));
        PG_RETURN_BOOL(false);
    }

    if(!get_chunk_table(chunk_id, schema, table)){
        ereport(ERROR, (errmsg("chunk %d not found", chunk_id)));
    }
    elog(NOTICE, "Decompressing chunk %d (%s.%s)…", chunk_id, schema, table);

    // read compressed data 
    SPI_connect();
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT column_name, column_type, algorithm, compressed_data "
        "FROM _timeseries_catalog.compressed_columns "
        "WHERE chunk_id = %d ORDER BY column_name", chunk_id);
    
    SPI_execute(query.data, true, 0);
    if(n_cols == 0){
        ereport(ERROR, (errmsg("no compressed column data found for chunk %d", chunk_id)));
    }

    // store data before SPI finish
    typedef struct{
        char name[NAMEDATALEN];
        Oid type;
        char algo[64];
        bytea *data;
    } ColInfo;
    ColInfo *cols = (ColInfo *) palloc(n_cols * sizeof(ColInfo));

    for(int32 i=0; i<n_cols; i++){
        bool isnull;
        strncpy(cols[i].name, TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull)), NAMEDATALEN);
        cols[i].type = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
        strncpy(cols[i].algo, TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull)), 64);
        cols[i].data = DatumGetByteaP(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull));
    }
    SPI_finish();

    // decompress each column
    typedef struct{
        char name[NAMEDATALEN];
        Oid type;
        int32 num_rows;
        text **text_vals;
        int64 **int_vals;
        int64 **ts_vals;
    } DecompressedCol;

    DecompressedCol *decompressed_cols = (DecompressedCol *) palloc(n_cols * sizeof(DecompressedCol));
    int32 n_rows = 0;
    for(int32 i=0; i<n_cols; i++){

        strncpy(decompressed_cols[i].name, cols[i].name, NAMEDATALEN);
        decompressed_cols[i].type = cols[i].type;

        if(strcmp(cols[i].algo, ALG_DICTIONARY) == 0){
            DictCompressed *compressed = dict_deserialise(cols[i].data);
            decompressed_cols[i].text_vals = decompress_text_column_from_dictionary(compressed);
            decompressed_cols[i].num_rows = compressed->num_rows;
        }
        else if(strcmp(cols[i].algo, ALG_DELTA) == 0){
            DeltaCompressed *compressed = delta_deserialise(cols[i].data);
            decompressed_cols[i].int_vals = decompress_int_column_from_delta(compressed);
            decompressed_cols[i].num_rows = compressed->num_rows;
        }
        else if (strcmp(cols[i].algo, ALG_DELTA_OF_DELTA) == 0)
        {
            DodCompressed *compressed = dod_deserialise(cols[i].data);
            decompressed_cols[i].ts_vals = decompress_timestamp_column_from_dod(compressed);
            decompressed_cols[i].num_rows = compressed->num_rows;
        }

        if(n_rows == 0){
            n_rows = decompressed_cols[i].num_rows;
        }
    }

    // create table
    SPI_connect();

    initStringInfo(&query);
    appendStringInfo(&query, "CREATE TABLE %s.%s (", quote_identifier(schema), quote_identifier(table));
    for(int32 i=0; i<n_cols; i++){
        if (i > 0) {
            appendStringInfoString(&query, ", ");
        }

        appendStringInfo(&query, 
            "%s %s",
            quote_identifier(decompressed_cols[i].name),
            format_type_be(decompressed_cols[i].type));
    }

    appendStringInfoString(&query, ")");
    SPI_execute(query.data, false, 0);

    // insert rows
    for(int32 row=0; row < n_rows; row++){
        resetStringInfo(&query);
        appendStringInfo(&query, "INSERT INTO %s.%s VALUES (",
            quote_identifier(schema), quote_identifier(table));

        for (int32 col = 0; col < n_cols; col++)
        {
            if (col > 0) appendStringInfoString(&query, ", ");

            if (strcmp(cols[col].algo, ALG_DICTIONARY) == 0){
                /* TEXT → quote literal */
                char *s = TextDatumGetCString(PointerGetDatum(decompressed_cols[col].text_vals[row]));
                appendStringInfo(&query, "'%s'", s);   /* simple quoting — production ใช้ quote_literal */
            }
            else if (strcmp(cols[col].algo, ALG_DELTA) == 0){
                appendStringInfo(&query, "%ld", decompressed_cols[col].int_vals[row]);
            }
            else if (strcmp(cols[col].algo, ALG_DELTA_OF_DELTA) == 0){
                /* TIMESTAMPTZ from int64 microseconds */
                appendStringInfo(&query, "'%s'::timestamptz",
                    timestamptz_to_str((TimestampTz) decompressed_cols[col].ts_vals[row]));
            }
        }
        appendStringInfoString(&query, ")");
        SPI_execute(query.data, false, 0);
    }

    // clean metadata
    resetStringInfo(&query);
    appendStringInfo(&query,
        "DELETE FROM _timeseries_catalog.compressed_columns WHERE chunk_id = %d", chunk_id);
    SPI_execute(query.data, false, 0);

    SPI_finish();

    mark_chunk_compressed(chunk_id, false);
    elog(NOTICE, "✅ Chunk %d decompressed — %d rows restored", chunk_id, n_rows);

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
    int compressed_count = 0;
    StringInfoData query;
    int64 cutoff_time;
    bool isnull;

    SPI_connect();

    // get hypertable id
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id "
        "FROM _timeseries_catalog.hypertable "
        "WHERE schema_name = '%s' AND table_name = '%s'",
        schema_name, table_name);
    
    int ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, (errmsg("\"%s\" is not a hypertable", table_name)));
    }

    hypertable_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull)); 

    // calculate cutoff time
    TimestampTz now = GetCurrentTimestamp();
    cutoff_time = now - (older_than->time + older_than->day * USECS_PER_DAY);
    elog(NOTICE, "Compressing chunks older than %s…", timestamptz_to_str(cutoff_time));

    // find chunk that need to compress
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id "
        "FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id = %d "
        "AND end_time < " INT64_FORMAT " "
        "AND (compressed IS NULL OR compressed = false) "
        "ORDER BY start_time",
        hypertable_id, cutoff_time);

    SPI_execute(query.data, true, 0);

    // store id before compress
    int32 n_chunks = (int32) SPI_processed;
    int32 *chunk_ids = (int32 *) palloc(n_chunks * sizeof(int32));
    for(int32 i=0; i<n_chunks; i++){
        bool isnull;
        chunk_ids[i] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
    }

    SPI_finish();
    
    // compress
    for(int32 i=0; i<n_chunks; i++){
        DirectFunctionCall1(compress_chunk, Int32GetDatum(chunk_ids[i]));
        compressed_count++;
    }

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
    int ret;
    bool isnull;
    StringInfoData query, result;

    SPI_connect();

    // get hypertable id
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id "
        "FROM _timeseries_catalog.hypertable "
        "WHERE schema_name = '%s' AND table_name = '%s'",
        schema_name, table_name);
    
    ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, (errmsg("\"%s\" is not a hypertable", table_name)));
    }
    
    hypertable_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));    

    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT "
        "  COUNT(*) as total_chunks, "
        "  COUNT(*) FILTER (WHERE compressed = true) as compressed_chunks, "
        "  COUNT(*) FILTER (WHERE compressed = false OR compressed IS NULL) as uncompressed_chunks "
        "FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id = %d", hypertable_id);
    SPI_execute(query.data, true, 0);

    initStringInfo(&result);
    if (SPI_processed > 0){
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