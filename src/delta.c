#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <funcapi.h>
#include <string.h>
#include <limits.h>

#include "delta.h"

DeltaCompressed *
compress_int_column_with_delta(const char *column_name,
                               Oid column_type,
                               int64 *values,
                               int32 num_rows)
{
    DeltaCompressed *compressed;
    int64 min_delta = INT64_MAX, max_delta = INT64_MIN;
    int64 original_size, compressed_size;
    int bytes_per_delta;

    // allocate
    compressed = (DeltaCompressed *) palloc0(sizeof(DeltaCompressed));
    strncpy(compressed->column_name, column_name, NAMEDATALEN);
    compressed->column_type = column_type;
    compressed->num_rows = num_rows;
    compressed->base_value = values[0]; // base value

    if(num_rows <= 1){
        compressed->deltas = NULL;
        elog(NOTICE, "  [Delta] %s: single row", column_name);
        return compressed;
    }

    compressed->deltas = (int64 *) palloc((num_rows-1) * sizeof(int64));
    for(int32 i=0; i<num_rows-1; i++){
        int64 diff = values[i+1] - values[i];
        compressed->deltas[i] = diff;
        if (diff < min_delta) min_delta = diff;
        if (diff > max_delta) max_delta = diff;
    }

    /*
        In original we stored each row for 8 bytes and when compressed, it also stored in 8 bytes (int64),
        To optimize this, we can decided that when smaller delta range it pack to int16/int32, to save the storage
    */
    bytes_per_delta = (min_delta >= -32768 && max_delta <= 32767) ? 2 :
                    (min_delta >= -2147483648LL && max_delta <= 2147483647LL) ? 4 : 8;
    
    original_size   = (int64)num_rows * 8;
    compressed_size = 8 + (int64)(num_rows - 1) * bytes_per_delta; // base value size + array size (exclude base value)

    elog(NOTICE, "  [Delta] %s: base=%ld  delta_range=[%ld..%ld]  est %.1f%%",
        column_name, compressed->base_value, min_delta, max_delta,
        (1.0 - (double)compressed_size / (double)original_size) * 100.0);

    return compressed;
}


int64 *
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


bytea *
delta_serialise(DeltaCompressed *c)
{
    int32 n_deltas = (c->num_rows > 1) ? c->num_rows-1 : 0;
    int32 total = 4 + 4 + 8 + (n_deltas * (int32)sizeof(int64));
    bytea *out = (bytea *) palloc(VARHDRSZ + total);
    char *p; // moving pointer

    SET_VARSIZE(out, VARHDRSZ + total);
    p = VARDATA(out);

    memcpy(p, &c->num_rows, 4); p+=4;
    memcpy(p, &c->column_type, 4); p+=4;
    memcpy(p, &c->base_value, 8); p+=8;
    
    if (n_deltas > 0){
        memcpy(p, c->deltas, n_deltas * sizeof(int64));
    }
    return out;
}

DeltaCompressed *
delta_deserialise(bytea *data)
{
    DeltaCompressed *c = (DeltaCompressed *)palloc0 (sizeof(DeltaCompressed));
    char *p = VARDATA(data);

    memcpy(&c->num_rows, p, 4); p+=4;
    memcpy(&c->column_type, p, 4); p+=4;
    memcpy(&c->base_value, p, 8); p+=8;

    int32 n_deltas = (c->num_rows > 1) ? c->num_rows - 1 : 0;
    if (n_deltas > 0){
        c->deltas = (int64 *) palloc(n_deltas * sizeof(int64));
        memcpy(c->deltas, p, n_deltas * sizeof(int64));
    }
    return c;
}