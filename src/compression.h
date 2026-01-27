#pragma once

#include <postgres.h>

extern Datum compress_chunk(PG_FUNCTION_ARGS);
extern Datum decompress_chunk(PG_FUNCTION_ARGS);
extern Datum compress_chunks_older_than(PG_FUNCTION_ARGS);
extern Datum show_chunk_compression_stats(PG_FUNCTION_ARGS);