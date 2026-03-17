#pragma once

#include <postgres.h>

#define NAMEDATALEN 64
#define MICROSECS_PER_DAY INT64CONST(86400000000)

// drop old chunk that older than cutoff time
extern int retention_drop_old_chunks(int hypertable_id, int64 cutoff_time);

// set, drop retention policy
extern void retention_set_policy(int hypertable_id, int64 retain_microseconds, char *retain_days);
extern void retention_drop_policy(int hypertable_id);

// run though all hypertable and remove by policy
extern int retention_apply_all_policies(void);

// background worker entry point
extern void retention_worker_main(Datum main_arg);