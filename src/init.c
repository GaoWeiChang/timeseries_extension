#include <postgres.h>
#include <fmgr.h>
#include <postmaster/bgworker.h>

#include "planner.h"
#include "retention.h"

PG_MODULE_MAGIC;

// this will start when load extension (CREATE EXTENSION)
void _PG_init(void){
    elog(LOG, "timeseries extension loaded successfully.");

    // planner hook
    planner_hook_init();

    // background worker
    BackgroundWorker retention_worker;
    MemSet(&retention_worker, 0, sizeof(retention_worker));

    snprintf(retention_worker.bgw_name, BGW_MAXLEN, "timeseries retention worker");
    snprintf(retention_worker.bgw_type, BGW_MAXLEN, "timeseries retention");
    snprintf(retention_worker.bgw_library_name, BGW_MAXLEN, "simple_timeseries");
    snprintf(retention_worker.bgw_function_name, BGW_MAXLEN, "retention_worker_main");

    retention_worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    retention_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    retention_worker.bgw_restart_time = 10;
    retention_worker.bgw_main_arg = Int32GetDatum(0);
    retention_worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&retention_worker);

    elog(LOG, "timeseries retention background worker registered.");
}

void _PG_fini(void){
    planner_hook_cleanup();
    elog(LOG, "timeseries extension unloaded.");
}