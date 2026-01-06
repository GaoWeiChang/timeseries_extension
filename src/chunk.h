#pragma once

#include <postgres.h>

extern int64 chunk_calculate_start(int64 time_point, int64 chunk_interval);
extern int64 calculate_chunk_end(int64 chunk_start, int64 chunk_interval);
extern int chunk_create(int hypertable_id, int64 time_point);
extern int chunk_find_for_timestamp(int hypertable_id, int64 timestamp);
extern int chunk_get_or_create(int hypertable_id, int64 timestamp);
