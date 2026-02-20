#include <postgres.h>
#include <fmgr.h>
#include <postmaster/bgworker.h>

#include "planner.h"
#include "retention.h"
#include "../tsl/src/continuous_aggs.h"

PG_MODULE_MAGIC;

// this will start when load extension (CREATE EXTENSION)
void _PG_init(void){
    BackgroundWorker worker;
    
    elog(LOG, "timeseries extension loaded successfully.");
    // planner hook
    planner_hook_init();

    // background worker : Data retention
    MemSet(&worker, 0, sizeof(worker));
    strlcpy(worker.bgw_name, "timeseries retention worker", BGW_MAXLEN);
    strlcpy(worker.bgw_type, "timeseries retention", BGW_MAXLEN);
    strlcpy(worker.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "retention_worker_main", BGW_MAXLEN);

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = Int32GetDatum(0);
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
    elog(LOG, "timeseries retention background worker registered.");

    // background worker : Continuous aggregate
    MemSet(&worker, 0, sizeof(worker));
    strlcpy(worker.bgw_name, "timeseries continuous aggregate worker", BGW_MAXLEN);
    strlcpy(worker.bgw_type, "timeseries continuous aggregate", BGW_MAXLEN);
    strlcpy(worker.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "cagg_worker_main", BGW_MAXLEN);

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = Int32GetDatum(0);
    worker.bgw_notify_pid = 0;
    
    RegisterBackgroundWorker(&worker);
    elog(LOG, "continuous aggregate worker registered");
}

void _PG_fini(void){
    planner_hook_cleanup();
    elog(LOG, "timeseries extension unloaded.");
}