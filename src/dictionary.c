#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <funcapi.h>
#include <string.h>

#include "dictionary.h"

DictCompressed *
compress_text_column_with_dictionary(const char *column_name, 
                                     text **values, 
                                     int num_rows)
{
    DictCompressed *compressed;
    int dict_capacity = 100;
    int dict_size = 0; // also used as dictionary position index

    // allocate memory
    compressed = (DictCompressed *) palloc0(sizeof(DictCompressed));
    strncpy(compressed->column_name, column_name, NAMEDATALEN);
    compressed->num_rows = num_rows;
    compressed->dictionary = (DictionaryEntry *) palloc(dict_capacity * sizeof(DictionaryEntry));
    compressed->encoded_values = (int32 *) palloc(num_rows * sizeof(int32));

    // build dictionary
    for(int i=0; i<num_rows; i++){
        char *value_str = TextDatumGetCString(PointerGetDatum(values[i]));
        int dict_id = -1;

        for(int j=0; j<dict_size; j++){
            if (strcmp(compressed->dictionary[j].value, value_str) == 0){
                dict_id = j;
                break;
            }
        }

        if(dict_id == -1){
            // expand disk if full
            if(dict_size >= dict_capacity){
                dict_capacity *=  2;
                compressed->dictionary = (DictionaryEntry *) repalloc(compressed->dictionary, 
                                                                    (dict_capacity * sizeof(DictionaryEntry)));
            }

            dict_id = dict_size;
            compressed->dictionary[dict_id].id = dict_id;
            strncpy(compressed->dictionary[dict_id].value, value_str, 256);
            dict_size += 1;
        }
        compressed->encoded_values[i] = dict_id;
    }

    compressed->dict_size = dict_size;

    elog(NOTICE, "Dictionary compress %s: %d unique / %d rows", column_name, dict_size, num_rows);
    return compressed;
}

text **
decompress_text_column_from_dictionary(DictCompressed *compressed)
{
    text **values;
    values = (text **) palloc(compressed->num_rows * sizeof(text *));

    for(int i=0; i < compressed->num_rows; i++){
        int32 dict_id = compressed->encoded_values[i];
        values[i] =  cstring_to_text(compressed->dictionary[dict_id].value);
    }

    return values;
}

bytea *
dict_serialise(DictCompressed *c)
{
    int32 dict_bytes = c->dict_size * 256;
    int32 id_bytes = c->num_rows * sizeof(int32);
    int32 total = 4 + 4 + dict_bytes + id_bytes; // dict_size + num_rows + dict_bytes + id_bytes
    bytea *out = (bytea *) palloc(VARHDRSZ + total);
    char *p; // moving pointer

    SET_VARSIZE(out, VARHDRSZ + total);
    p = VARDATA(out);

    memcpy(p, &c->dict_size, 4); p+=4;
    memcpy(p, &c->num_rows, 4); p+=4;
    
    for(int32 i=0; i<c->dict_size; i++){
        memcpy(p, c->dictionary[i].value, 256);
        p += 256;
    }
    memcpy(p, c->encoded_values, id_bytes);

    return out;
}

DictCompressed *
dict_deserialise(bytea *data)
{
    DictCompressed *c = (DictCompressed *) palloc(sizeof(DictCompressed));
    char *p = VARDATA(data);

    memcpy(&c->dict_size, p, 4); p+=4;
    memcpy(&c->num_rows, p, 4); p+=4;
   
    c->dictionary = (DictionaryEntry *) palloc(c->dict_size * sizeof(DictionaryEntry));
    for(int32 i=0; i<c->dict_size; i++){
        c->dictionary[i].id = i;
        memcpy(c->dictionary[i].value, p, 256);
        p += 256;
    }

    c->encoded_values = (int32 *) palloc(c->num_rows * sizeof(int32));
    memcpy(c->encoded_values, p, c->num_rows * sizeof(int32));

    return c;
}