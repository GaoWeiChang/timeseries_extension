#include <postgres.h>
#include <fmgr.h>
#include <postmaster/bgworker.h>

#include "planner.h"

PG_MODULE_MAGIC;

// this will start when load extension (CREATE EXTENSION)
void _PG_init(void){
    BackgroundWorker worker;
    
    elog(LOG, "timeseries extension loaded successfully.");
    // planner hook
    planner_hook_init();
}

void _PG_fini(void){
    planner_hook_cleanup();
    elog(LOG, "timeseries extension unloaded.");
}