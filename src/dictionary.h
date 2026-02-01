#pragma once

#include <postgres.h>

extern Datum test_compress_chunk_column(PG_FUNCTION_ARGS);
extern Datum show_compression_info(PG_FUNCTION_ARGS);