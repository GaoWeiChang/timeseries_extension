#pragma once

#include <postgres.h>

typedef struct ChunkInfo {
    int chunk_id;
    char schema_name[NAMEDATALEN];
    char table_name[NAMEDATALEN];
    int64 start_time;
    int64 end_time;
} ChunkInfo;

extern int64 chunk_calculate_start(int64 time_point, int64 chunk_interval);
extern int64 chunk_calculate_end(int64 chunk_start, int64 chunk_interval);
extern ChunkInfo* chunk_create(int hypertable_id, int64 time_point);
extern ChunkInfo* chunk_get_or_create(int hypertable_id, int64 timestamp);
