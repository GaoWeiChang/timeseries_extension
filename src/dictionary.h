#pragma once

#include <postgres.h>
#include <catalog/pg_type.h>

typedef struct DictionaryEntry
{
    int32 id;
    char value[256];
} DictionaryEntry;

typedef struct DictCompressed
{
    char column_name[NAMEDATALEN];
    int32 dict_size;
    DictionaryEntry *dictionary; // array[dict_size]
    int32 num_rows;
    int32 *encoded_values; // array[num_rows]
} DictCompressed;


// core (compress data in main memory)
extern DictCompressed *compress_text_column_with_dictionary(const char *column_name,
                                                            char **values,
                                                            int32 num_rows);
extern text **decompress_text_column_from_dictionary(DictCompressed *compressed);


// serialise (store in disk)
extern bytea *dict_serialise(DictCompressed *compressed);
extern DictCompressed *dict_deserialise(bytea *data);