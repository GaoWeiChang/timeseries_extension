#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <funcapi.h>
#include <string.h>
#include <limits.h>

#include "delta_of_delta.h"


DodCompressed *
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
    pfree(deltas);
    

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

    return compressed;
}

int64 *
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

bytea *
dod_serialise(DodCompressed *c)
{
    int32 n_dod = (c->num_rows > 2) ? c->num_rows - 2 : 0;
    int32 total = 4 + 8 + 8 + (n_dod * (int32)sizeof(int64));
    bytea *out = (bytea *) palloc(VARHDRSZ + total);
    char *p;

    SET_VARSIZE(out, VARHDRSZ + total);
    p = VARDATA(out);

    memcpy(p, &c->num_rows, 4); p += 4;
    memcpy(p, &c->base_timestamp, 8); p += 8;
    memcpy(p, &c->first_delta, 8); p += 8;
    if (n_dod > 0){
        memcpy(p, c->dod, n_dod * sizeof(int64));
    }

    return out;
}

DodCompressed *
dod_deserialise(bytea *data)
{
    DodCompressed *c = (DodCompressed *) palloc0(sizeof(DodCompressed));
    char *p = VARDATA(data);

    memcpy(&c->num_rows, p, 4); p += 4;
    memcpy(&c->base_timestamp, p, 8); p += 8;
    memcpy(&c->first_delta, p, 8); p += 8;

    int32 n_dod = (c->num_rows > 2) ? c->num_rows - 2 : 0;
    if(n_dod > 0){
        c->dod = (int64 *) palloc(n_dod * sizeof(int64));
        memcpy(c->dod, p, n_dod * sizeof(int64));
    }
    
    return c;       
}