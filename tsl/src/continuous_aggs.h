#pragma once

#include <postgres.h>

#define NAMEDATALEN 64
#define MICROSECS_PER_DAY INT64CONST(86400000000)

extern void cagg_create(const char *cagg_name,
                        const char *hypertable_schema,
                        const char *hypertable_name,
                        const char *view_sql,
                        int64 bucket_width);

// refresh by range [start, end]
extern void cagg_refresh(int cagg_id, int64 start_time, int64 end_time);

// refresh all continuous aggregation that need to refresh
extern int cagg_refresh_all_due(void);

// get/update watermark
extern void cagg_set_watermark(int cagg_id, int64 watermark);

// background worker
extern void cagg_worker_main(Datum main_arg);