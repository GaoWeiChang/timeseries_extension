#pragma once

#include <postgres.h>

typedef struct DodCompressed
{
    char column_name[NAMEDATALEN];
    int64 base_timestamp;
    int64 first_delta;
    int32 num_rows;
    int64 *dod;    // array[num_rows-2]
} DodCompressed;


// core (compress data in main memory)
extern DodCompressed *compress_timestamp_column_with_dod(const char *column_name,
                                                        int64 *timestamps,
                                                        int32 num_rows);
extern int64 *decompress_timestamp_column_from_dod(DodCompressed *compressed);

// serialise (store in disk)dod_deserialise
extern bytea *dod_serialise(DodCompressed *c);
extern DodCompressed *dod_deserialise(bytea *data);