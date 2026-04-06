#include <postgres.h>
#include <fmgr.h>
#include <postmaster/bgworker.h>

#include "planner.h"
#include "launcher.h"

PG_MODULE_MAGIC;

// this will start when load extension (CREATE EXTENSION)
void _PG_init(void){

    // register launcher
    BackgroundWorker launcher;

    MemSet(&launcher, 0, sizeof(launcher));
    strlcpy(launcher.bgw_name, "simple_timeseries launcher", BGW_MAXLEN);
    strlcpy(launcher.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
    strlcpy(launcher.bgw_function_name, "launcher_main", BGW_MAXLEN);

    launcher.bgw_flags = BGWORKER_SHMEM_ACCESS | 
                        BGWORKER_BACKEND_DATABASE_CONNECTION; // access authority 
    launcher.bgw_start_time = BgWorkerStart_RecoveryFinished;
    launcher.bgw_restart_time = 5;
    launcher.bgw_main_arg = (Datum) 0;

    RegisterBackgroundWorker(&launcher);

    elog(LOG, "auto_job: launcher registered.");


    // planner hook
    planner_hook_init();
}

void _PG_fini(void){
    planner_hook_cleanup();
    elog(LOG, "timeseries extension unloaded.");
}