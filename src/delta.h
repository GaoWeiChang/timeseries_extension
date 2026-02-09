#pragma once

#include <postgres.h>

typedef struct DeltaCompressed
{
    char column_name[NAMEDATALEN];
    Oid column_type;
    int64 base_value;
    int64 *deltas;
    int32 num_rows;
} DeltaCompressed;


// core (compress data in main memory)
extern DeltaCompressed *compress_text_column_with_delta(const char *column_name,
                                                        Oid column_type,
                                                        int64 *values,
                                                        int32 num_rows);
extern int64 **decompress_text_column_from_delta(DeltaCompressed *compressed);


// serialise (store in disk)
extern bytea *delta_serialise(DeltaCompressed *compressed);
extern DeltaCompressed *delta_deserialise(bytea *data);

