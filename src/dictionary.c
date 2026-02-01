#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <access/htup_details.h>
#include <funcapi.h>

#include "metadata.h"
#include "chunk.h"

typedef struct DictionaryEntry
{
    int32 id;
    char value[256];
} DictionaryEntry;

typedef struct CompressedColumn
{
    char column_name[NAMEDATALEN];
    Oid column_type;

    int32 dict_size;
    DictionaryEntry *dictionary;
    int32 *encoded_values;
    int32 num_rows;
} CompressedColumn;

static CompressedColumn *
compress_text_column_with_dictionary(const char *column_name, 
                                     text **values, 
                                     int num_rows)
{
    CompressedColumn *compressed;
    int dict_capacity = 100;
    int dict_size = 0; // also used as dictionary position index

    // allocate memory
    compressed = (CompressedColumn *) palloc0(sizeof(CompressedColumn));
    strncpy(compressed->column_name, column_name, NAMEDATALEN);
    compressed->column_type = TEXTOID;
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

    elog(NOTICE, "Dictionary encoding: %s", column_name);
    elog(NOTICE, "Unique values: %d", dict_size);
    elog(NOTICE, "Total rows: %d", num_rows);
    elog(NOTICE, "Memory saved after compressed: %.1f%%", (1.0 - ((float)dict_size * 256 + num_rows * 4) / (num_rows * 256.0)) * 100.0);
    
    return compressed;
}

static text **
decompress_text_column_from_dictionary(CompressedColumn *compressed)
{
    text **values;
    values = (text **) palloc(compressed->num_rows * sizeof(text *));

    for(int i=0; i < compressed->num_rows; i++){
        int32 dict_id = compressed->encoded_values[i];
        char *original_value = compressed->dictionary[dict_id].value;
        values[i] =  cstring_to_text(original_value);
    }

    return values;
}

// compress single column
static CompressedColumn *
compress_chunk_column(const char *schema_name,
                      const char *table_name,
                      const char *column_name,
                      Oid column_type)
{
    StringInfoData query;
    CompressedColumn *compressed = NULL;

    SPI_connect();

    // get column data
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT %s FROM %s.%s ORDER BY ctid",
        quote_identifier(column_name), quote_identifier(schema_name), quote_identifier(table_name));

    int ret = SPI_execute(query.data, true, 0);
    if((ret != SPI_OK_SELECT) || (SPI_processed == 0)){
        SPI_finish();
        return NULL;
    }
    
    // select compression algorithm based on type
    int num_rows = SPI_processed;
    if((column_type == TEXTOID) || (column_type == VARCHAROID)){
        text **values = (text **) palloc(num_rows * sizeof(text *));
        for(uint64 i = 0; i < SPI_processed; i++){
            bool isnull;
            Datum value = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
            if (!isnull)
                values[i] = DatumGetTextPP(value);
            else
                values[i] = cstring_to_text("");
        }
        compressed = compress_text_column_with_dictionary(column_name, values, num_rows);
    } else {
        elog(WARNING, "  Column %s (type %u): No compression algorithm implemented yet", column_name, column_type);
    }

    SPI_finish();

    return compressed;
}


PG_FUNCTION_INFO_V1(test_compress_chunk_column);
Datum 
test_compress_chunk_column(PG_FUNCTION_ARGS)
{
    text *table_name_text = PG_GETARG_TEXT_PP(0);
    text *column_name_text = PG_GETARG_TEXT_PP(1);
    
    char *table_name = text_to_cstring(table_name_text);
    char *column_name = text_to_cstring(column_name_text);
    char *schema_name = "public";
    
    StringInfoData query;
    Oid column_type;
    CompressedColumn *compressed;

    elog(NOTICE, "Testing compression on %s.%s.%s...", schema_name, table_name, column_name);

    // get column name
    SPI_connect();
    
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT atttypid FROM pg_attribute "
        "WHERE attrelid = '%s.%s'::regclass AND attname = '%s'",
        schema_name, table_name, column_name);
    
    int ret = SPI_execute(query.data, true, 0);
    
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        SPI_finish();
        ereport(ERROR,
                (errmsg("column \"%s\" not found in table \"%s\"",
                        column_name, table_name)));
    }
    
    bool isnull;
    column_type = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    
    SPI_finish();

    // compress column
    compressed = compress_chunk_column(schema_name, table_name, column_name, column_type);

    if(compressed){
        elog(NOTICE, "Testing decompression...");
        text **decompressed = decompress_text_column_from_dictionary(compressed);
        
        elog(NOTICE, "✅ Compression test completed!");
        elog(NOTICE, "   First value (original):     %s",
             compressed->dictionary[compressed->encoded_values[0]].value);
        elog(NOTICE, "   First value (decompressed): %s",
             TextDatumGetCString(PointerGetDatum(decompressed[0])));
    }

    PG_RETURN_BOOL(compressed != NULL);
}


PG_FUNCTION_INFO_V1(show_compression_info);
Datum
show_compression_info(PG_FUNCTION_ARGS)
{
    text *table_name_text = PG_GETARG_TEXT_PP(0);
    char *table_name = text_to_cstring(table_name_text);
    char *schema_name = "public";
    
    StringInfoData result;
    StringInfoData query;
    
    initStringInfo(&result);
    appendStringInfo(&result, "Compression Info for %s.%s\n\n", schema_name, table_name);
    
    SPI_connect();
    
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT attname, atttypid::regtype "
        "FROM pg_attribute "
        "WHERE attrelid = '%s.%s'::regclass "
        "AND attnum > 0 AND NOT attisdropped "
        "ORDER BY attnum",
        schema_name, table_name);
    
    int ret = SPI_execute(query.data, true, 0);
    
    if (ret == SPI_OK_SELECT)
    {
        appendStringInfoString(&result, "Columns and compression algorithms:\n");
        
        for (uint64 i = 0; i < SPI_processed; i++)
        {
            // bool isnull;
            // Datum col_name = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
            // Datum col_type = SPI_getvalue(SPI_tuptable->vals[i],SPI_tuptable->tupdesc, 2, &isnull);
            
            // char *name = TextDatumGetCString(col_name);
            // char *type = TextDatumGetCString(col_type);

            char *name = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            char *type = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
            
            if (name == NULL || type == NULL)
                continue;

            appendStringInfo(&result, "  - %s (%s): ", name, type);
            
            /* Determine algorithm */
            if (strstr(type, "text") || strstr(type, "character"))
            {
                appendStringInfoString(&result, "Dictionary Encoding\n");
            }
            else if (strstr(type, "timestamp"))
            {
                appendStringInfoString(&result, "Delta-of-Delta (not implemented)\n");
            }
            else if (strstr(type, "integer") || strstr(type, "bigint"))
            {
                appendStringInfoString(&result, "Delta Encoding (not implemented)\n");
            }
            else if (strstr(type, "double") || strstr(type, "numeric"))
            {
                appendStringInfoString(&result, "No compression\n");
            }
            else
            {
                appendStringInfoString(&result, "Unknown\n");
            }
        }
    }
    
    SPI_finish();
    
    PG_RETURN_TEXT_P(cstring_to_text(result.data));
}