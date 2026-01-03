#include <postgres.h>
#include <fmgr.h>

PG_MODULE_MAGIC;

// this will start when load extension (CREATE EXTENSION)
void _PG_init(void){
    elog(LOG, "timeseries extension loaded successfully.");
}

void _PG_fini(void){
    elog(LOG, "timeseries extension unloaded.");
}