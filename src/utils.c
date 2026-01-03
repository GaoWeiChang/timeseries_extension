#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h> 
#include <utils/timestamp.h>

PG_FUNCTION_INFO_V1(get_current_timestamp_seconds);
Datum get_current_timestamp_seconds(PG_FUNCTION_ARGS){
    TimestampTz current_time = GetCurrentTimestamp();
    
    // convert microsecond to second
    int64 seconds = current_time / USECS_PER_SEC;
    
    PG_RETURN_INT64(seconds);
}