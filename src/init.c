#include <postgres.h>
#include <fmgr.h>

#include "planner.h"

PG_MODULE_MAGIC;

// this will start when load extension (CREATE EXTENSION)
void _PG_init(void){
    elog(LOG, "timeseries extension loaded successfully.");
    // planner_hook_init();
}

void _PG_fini(void){
    // planner_hook_cleanup();
    elog(LOG, "timeseries extension unloaded.");
}