#pragma once

#include <postgres.h>

#define NAMEDATALEN 64

typedef struct CompressedChunkInfo
{
    int chunk_id;
    int original_row_count;
    int64 uncompressed_bytes;
    int64 compressed_bytes;
    double compression_ratio;
    bool is_compressed;
} CompressedChunkInfo;

// compress chunk (column oriented)
extern CompressedChunkInfo* compress_chunk_internal(int chunk_id);
