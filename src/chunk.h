#pragma once

#include <postgres.h>
#include <utils/hsearch.h>

typedef struct ChunkInfo {
    int chunk_id;
    char schema_name[NAMEDATALEN];
    char table_name[NAMEDATALEN];
    int64 start_time;
    int64 end_time;
} ChunkInfo;

// cache key
typedef struct ChunkCacheKey {
    int hypertable_id;
    int64 chunk_start;
} ChunkCacheKey;

// data that stored in cache
typedef struct ChunkCacheEntry{
    ChunkCacheKey key;
    ChunkInfo info;
} ChunkCacheEntry;


extern int64 chunk_calculate_start(int64 time_point, int64 chunk_interval);
extern int64 chunk_calculate_end(int64 chunk_start, int64 chunk_interval);
extern ChunkInfo* chunk_create(int hypertable_id, int64 time_point);
extern ChunkInfo* chunk_get_or_create(int hypertable_id, int64 timestamp);
extern void chunk_drop_all_chunk(const char *schema_name, const char *table_name);


void chunk_cache_init(void);