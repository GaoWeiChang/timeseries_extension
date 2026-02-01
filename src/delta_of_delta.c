#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <funcapi.h>

typedef struct DodCompressed
{
    char column_name[NAMEDATALEN];
    int64  base_timestamp;
    int64  first_delta;      // timestamps[1] - timestamps[0] 
    int64 *dod;              // length = num_rows - 2 (หา delta หนึ่งครั้ง เสียหนึ่งตัว หา delta อีกครั้ง เสียอีกหนึ่งตัว)  
    int32  num_rows;
} DodCompressed;

static DodCompressed *
compress_timestamp_column_with_dod(const char *column_name,
                                   int64 *timestamps,
                                   int32 num_rows)
{
    DodCompressed *compressed;
    int64 *deltas; // delta for dod
    int64  min_dod, max_dod, zero_count;

    compressed = (DodCompressed *) palloc0(sizeof(DodCompressed));
    strncpy(compressed->column_name, column_name, NAMEDATALEN);
    compressed->base_timestamp = timestamps[0];
    compressed->num_rows = num_rows;

    if(num_rows == 1){
        compressed->first_delta = 0;
        compressed->dod = NULL;
        elog(NOTICE, "  Delta-of-Delta: %s (single row)", column_name);
        return compressed;
    }

    compressed->first_delta = timestamps[1] - timestamps[0];
    if(num_rows == 2){
        compressed->dod = NULL;
        return compressed;
    }

    // calculate delta
    deltas = (int64 *) palloc((num_rows-1) * sizeof(int64));
    for(int32 i=0; i<num_rows-1; i++){
        deltas[i] = timestamps[i+1] - timestamps[i];
    }

    // calculate dod from delta
    compressed->dod = (int64 *) palloc((num_rows-2) * sizeof(int64));
    min_dod = INT64_MAX;
    max_dod = INT64_MIN;
    zero_count = 0;

    for(int32 i=0; i<num_rows-2; i++){
        int64 diff = deltas[i+1] - deltas[i];
        compressed->dod[i] = diff;

        if(diff < min_dod) min_dod = diff;
        if(diff > max_dod) max_dod = diff;
        if(diff == 0) zero_count+=1;
    }

    // report
    elog(NOTICE, "Delta-of-Delta: %s", column_name);
    elog(NOTICE, "Rows: %d", num_rows);
    elog(NOTICE, "Base timestamp: %ld", compressed->base_timestamp);
    elog(NOTICE, "First delta: %ld µs", compressed->first_delta);
    elog(NOTICE, "DoD min: %ld", min_dod);
    elog(NOTICE, "DoD max: %ld", max_dod);
    elog(NOTICE, "DoD zeros: %ld / %d (%.1f%%)",
                            zero_count, num_rows - 2,
                            (double)zero_count / (num_rows - 2) * 100.0);
    
    {
        int bytes_per_dod;
        if (min_dod >= -32768 && max_dod <= 32767)
            bytes_per_dod = 2;
        else if (min_dod >= -2147483648LL && max_dod <= 2147483647LL)
            bytes_per_dod = 4;
        else
            bytes_per_dod = 8;

        int64  original_size   = (int64)num_rows * 8;
        int64  compressed_size = 8 + 8 + ((int64)(num_rows - 2) * bytes_per_dod);  // base + first delta + dod array
        double ratio = (1.0 - (double)compressed_size / (double)original_size) * 100.0;

        elog(NOTICE, "DoD fits in: int%d (%d bytes each)", bytes_per_dod * 8, bytes_per_dod);
        elog(NOTICE, "Compression: %.1f%% (estimate)", ratio);
    }

    pfree(deltas);
    return compressed;
}

static int64 *
decompress_timestamp_column_from_dod(DodCompressed *compressed)
{
    int64 *timestamps;
    int64 cur_delta;
    int32 num_rows = compressed->num_rows;

    timestamps = (int64 *) palloc(num_rows * sizeof(int64));

    timestamps[0] = compressed->base_timestamp; // base

    if (num_rows == 1)
        return timestamps;

    cur_delta = compressed->first_delta;
    timestamps[1] = timestamps[0] + cur_delta;

    for (int32 i=0; i < num_rows-2; i++){
        cur_delta += compressed->dod[i];
        timestamps[i+2]  = timestamps[i+1] + cur_delta;
    }

    return timestamps;
}


PG_FUNCTION_INFO_V1(test_compress_dod);
Datum
test_compress_dod(PG_FUNCTION_ARGS)
{
    text *table_name_text  = PG_GETARG_TEXT_PP(0);
    text *column_name_text = PG_GETARG_TEXT_PP(1);

    char *table_name  = text_to_cstring(table_name_text);
    char *column_name = text_to_cstring(column_name_text);
    char *schema_name = "public";

    StringInfoData   query;
    int64           *original_ts;
    int32            num_rows;
    DodCompressed   *compressed;
    int64           *decompressed_ts;
    bool             all_match = true;
    
    elog(NOTICE, "=== Delta-of-Delta Test: %s.%s ===", table_name, column_name);

    // read column
    SPI_connect();

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT %s FROM %s.%s ORDER BY ctid",
        quote_identifier(column_name),
        quote_identifier(schema_name),
        quote_identifier(table_name));
    
    int ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, (errmsg("no data in %s.%s", table_name, column_name)));
    }

    num_rows = (int32) SPI_processed;
    original_ts = (int64 *) palloc(num_rows * sizeof(int64));
    for (int32 i = 0; i < num_rows; i++){
        bool isnull;
        Datum val = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
        original_ts[i] = isnull ? 0 : DatumGetInt64(val);
    }
    
    SPI_finish();

    // compress
    elog(NOTICE, "Compressing %d rows...", num_rows);
    compressed = compress_timestamp_column_with_dod(column_name, original_ts, num_rows);

    // decompress
    elog(NOTICE, "Decompressing...");
    decompressed_ts = decompress_timestamp_column_from_dod(compressed);

    //verify
    for (int32 i = 0; i < num_rows; i++){
        if (original_ts[i] != decompressed_ts[i])
        {
            elog(NOTICE, "  ❌ MISMATCH at row %d: original=%ld, decompressed=%ld",
                 i, original_ts[i], decompressed_ts[i]);
            all_match = false;
        }
    }

    if (all_match){
        elog(NOTICE, "✅ Verification passed! All %d timestamps match.", num_rows);
        elog(NOTICE, "   First 5 original:     ");
        for (int32 i = 0; i < 5 && i < num_rows; i++)
            elog(NOTICE, "     [%d] = %ld  (%s)",
                 i, original_ts[i], timestamptz_to_str((TimestampTz)original_ts[i]));
        elog(NOTICE, "   First 5 decompressed: ");
        for (int32 i = 0; i < 5 && i < num_rows; i++)
            elog(NOTICE, "     [%d] = %ld  (%s)",
                 i, decompressed_ts[i], timestamptz_to_str((TimestampTz)decompressed_ts[i]));
    }
    else{
        elog(NOTICE, "❌ Verification FAILED!");
    }

    PG_RETURN_BOOL(all_match);
}