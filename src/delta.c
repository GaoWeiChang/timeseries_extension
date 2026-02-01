#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <funcapi.h>


typedef struct DeltaCompressed
{
    char column_name[NAMEDATALEN];
    Oid column_type;

    int64  base_value;  // first value
    int64 *deltas;      // changes of each row
    int32  num_rows;
} DeltaCompressed;

static DeltaCompressed *
compress_int_column_with_delta(const char *column_name,
                               Oid column_type,
                               int64 *values,
                               int32 num_rows)
{
    DeltaCompressed *compressed;
    int64 min_delta, max_delta, sum_abs_delta;

    // allocate
    compressed = (DeltaCompressed *) palloc0(sizeof(DeltaCompressed));
    strncpy(compressed->column_name, column_name, NAMEDATALEN);
    compressed->column_type = column_type;
    compressed->num_rows    = num_rows;

    compressed->base_value = values[0]; // base value

    if(num_rows > 1){
        compressed->deltas = (int64 *) palloc((num_rows-1) * sizeof(int64));
        min_delta = INT64_MAX;
        max_delta = INT64_MIN;
        sum_abs_delta = 0;    

        for(int32 i=0; i<num_rows-1; i++){
            int64 diff = values[i+1] - values[i];
            compressed->deltas[i] = diff;

            if(diff < min_delta) min_delta = diff;
            if(diff > max_delta) max_delta = diff;
            sum_abs_delta += (diff < 0) ? -diff : diff; // for heuristic
        }
        elog(NOTICE, "Delta Encoding: %s", column_name);
        elog(NOTICE, "Rows: %d", num_rows);
        elog(NOTICE, "Base value: %ld", compressed->base_value);
        elog(NOTICE, "Min delta: %ld", min_delta);
        elog(NOTICE, "Max delta: %ld", max_delta);
        elog(NOTICE, "Avg |delta|: %ld", sum_abs_delta / (num_rows - 1));

        /*
            In original we stored each row for 8 bytes and when compressed, it also stored in 8 bytes (int64),
            To optimize this, we can decided that when smaller delta range it pack to int16/int32, to save the storage
        */
        {
            int bytes_per_delta;
            if (min_delta >= -32768 && max_delta <= 32767)
                bytes_per_delta = 2;    // int16
            else if (min_delta >= -2147483648LL && max_delta <= 2147483647LL)
                bytes_per_delta = 4;    // int32
            else
                bytes_per_delta = 8;    // int64 

            int64 original_size   = (int64)num_rows * 8;
            int64 compressed_size = 8 + (int64)(num_rows - 1) * bytes_per_delta; // base value size + array size (exclude base value)
            double ratio = (1.0 - (double)compressed_size / (double)original_size) * 100.0;
            elog(NOTICE, "Delta fits in: int%d (%d bytes each)", bytes_per_delta*8, bytes_per_delta);
            elog(NOTICE, "Compression: %.1f%% (estimate)", ratio);
        }
    } 
    else {
        compressed->deltas = NULL;
        elog(NOTICE, "  Delta Encoding: %s (single row, no deltas)", column_name);
    }

    return compressed;
}


static int64 *
decompress_int_column_from_delta(DeltaCompressed *compressed)
{
    int64 *values;
    int32  num_rows = compressed->num_rows;
    values = (int64 *) palloc(num_rows * sizeof(int64));

    values[0] = compressed->base_value;

    for(int32 i=1; i<num_rows; i++){
        values[i] = values[i-1] + compressed->deltas[i-1];
    }

    return values;
}

PG_FUNCTION_INFO_V1(test_compress_delta);
Datum
test_compress_delta(PG_FUNCTION_ARGS)
{
    text *table_name_text = PG_GETARG_TEXT_PP(0);
    text *column_name_text = PG_GETARG_TEXT_PP(1);

    char *table_name = text_to_cstring(table_name_text);
    char *column_name = text_to_cstring(column_name_text);
    char *schema_name = "public";

    StringInfoData query;
    int64 *original_values;
    int32 num_rows;
    Oid column_type;
    DeltaCompressed *compressed;
    int64 *decompressed_values;
    bool all_match = true;

    elog(NOTICE, "=== Delta Encoding Test: %s.%s ===", table_name, column_name);

    SPI_connect();

    // get column type
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT atttypid FROM pg_attribute "
        "WHERE attrelid = '%s.%s'::regclass AND attname = '%s'",
        schema_name, table_name, column_name);
    
    int ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, errmsg("column \"%s\" not found in table \"%s\"", column_name, table_name));
    }

    {
        bool isnull;
        column_type = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    }

    // read column data
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT %s FROM %s.%s ORDER BY ctid",
        quote_identifier(column_name),
        quote_identifier(schema_name),
        quote_identifier(table_name));

    ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, (errmsg("no data in column \"%s\"", column_name)));
    }

    num_rows = (int32) SPI_processed;
    original_values = (int64 *) palloc(num_rows * sizeof(int64));

    for(int32 i=0; i<num_rows; i++){
        bool isnull;
        Datum val = SPI_getbinval(SPI_tuptable->vals[i],
                                  SPI_tuptable->tupdesc, 1, &isnull);
        if (isnull)
            original_values[i] = 0;
        else if (column_type == INT4OID)
            original_values[i] = (int64) DatumGetInt32(val);
        else
            original_values[i] = DatumGetInt64(val);
    }
    SPI_finish();
    
    elog(NOTICE, "Compressing %d rows...", num_rows);
    compressed = compress_int_column_with_delta(column_name, column_type, original_values, num_rows);
    
    elog(NOTICE, "Decompressing...");
    decompressed_values = decompress_int_column_from_delta(compressed);
    
    // verify
    for (int32 i = 0; i < num_rows; i++){
        if (original_values[i] != decompressed_values[i])
        {
            elog(NOTICE, " ❌ MISMATCH at row %d: original=%ld, decompressed=%ld",
                 i, original_values[i], decompressed_values[i]);
            all_match = false;
        }
    }

    if (all_match){
        elog(NOTICE, "✅ Verification passed! All %d values match.", num_rows);
        elog(NOTICE, "   First 5 original:     ");
        for (int32 i = 0; i < 5 && i < num_rows; i++)
            elog(NOTICE, "     [%d] = %ld", i, original_values[i]);
        elog(NOTICE, "   First 5 decompressed: ");
        for (int32 i = 0; i < 5 && i < num_rows; i++)
            elog(NOTICE, "     [%d] = %ld", i, decompressed_values[i]);
    }
    else
    {
        elog(NOTICE, "❌ Verification FAILED!");
    }

    PG_RETURN_BOOL(all_match);
}