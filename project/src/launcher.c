#include <postgres.h>
#include <fmgr.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <executor/spi.h>
#include <utils/snapmgr.h>
#include <tcop/utility.h>

#include "launcher.h"
#include "../tsl/src/continuous_aggs.h"
#include "../tsl/src/retention.h"


// bgw signal handler
static volatile sig_atomic_t got_sigterm = false;

static void
launcher_sigterm_handler(SIGNAL_ARGS)
{
    got_sigterm = true;
    SetLatch(MyLatch);
}


/*
    Private function
*/

static bool
is_specific_worker_running(Oid db_oid, const char *app_name)
{
    int ret;
    bool exists = false;
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT 1 FROM pg_stat_activity "
        "WHERE application_name = '%s' "
        "  AND datid = %u", app_name, db_oid);

    ret = SPI_execute(query.data, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
        exists = true;

    return exists;
}


// spawn cagg and retention workers for a specific database
static void
spawn_worker(Oid db_oid)
{
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;

    // spawn continuous aggregate worker if not running
    if (!is_specific_worker_running(db_oid, "continuous aggregate worker"))
    {
        MemSet(&worker, 0, sizeof(worker));
        strlcpy(worker.bgw_name, "continuous aggregate worker", BGW_MAXLEN);
        strlcpy(worker.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
        strlcpy(worker.bgw_function_name, "cagg_worker_main", BGW_MAXLEN);
        
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = BGW_NEVER_RESTART;
        worker.bgw_main_arg = ObjectIdGetDatum(db_oid);
        
        RegisterDynamicBackgroundWorker(&worker, &handle);
        elog(LOG, "launcher: spawned continuous aggregate worker for db oid=%u", db_oid);
    }

    // spawn retention worker if not running
    if (!is_specific_worker_running(db_oid, "retention worker"))
    {
        MemSet(&worker, 0, sizeof(worker));
        strlcpy(worker.bgw_name, "retention worker", BGW_MAXLEN);
        strlcpy(worker.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
        strlcpy(worker.bgw_function_name, "retention_worker_main", BGW_MAXLEN);
        
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = BGW_NEVER_RESTART;
        worker.bgw_main_arg = ObjectIdGetDatum(db_oid);
        
        RegisterDynamicBackgroundWorker(&worker, &handle);
        elog(LOG, "launcher: spawned retention worker for db oid=%u", db_oid);
    }
}


// remove registry entries and terminate workers for database that dropped extension
static void
launcher_cleanup(void)
{
    int ret;
    uint64 dropped_count;
    Oid *dropped_db;
    StringInfoData query;

    // Find db in registry where the db no longer exists or extension was dropped

    ret = SPI_execute(
        "SELECT r.db_oid "
        "FROM public._timeseries_workers r "
        "WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE oid = r.db_oid) "
        "   OR NOT EXISTS ("
        "       SELECT 1 FROM dblink("
        "           format('dbname=%s', r.db_name),"
        "           'SELECT 1 FROM pg_extension WHERE extname = ''simple_timeseries'''"
        "       ) AS t(val int)"
        "   )",
        true, 0);

    if(ret != SPI_OK_SELECT){
        elog(WARNING, "launcher: failed to check for dropped extensions");
        return;
    }
    dropped_count = SPI_processed;
    if (dropped_count == 0)
        return;
    
    // copy oid to prevent overwrite
    dropped_db = palloc(sizeof(Oid) * dropped_count);
    for(uint64 i=0; i < dropped_count; i++){
        bool isnull;
        dropped_db[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
    }

    for(uint64 i=0; i < dropped_count; i++){
        Oid db_oid = dropped_db[i];
        elog(LOG, "launcher: dropped job worker for db oid=%u", db_oid);

        initStringInfo(&query);

        // terminate job worker on that database
        appendStringInfo(&query,
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE application_name IN ('continuous aggregate worker', 'retention worker') "
            "   AND datid = %u", db_oid);
        SPI_execute(query.data, false, 0);

        // remove from registry
        resetStringInfo(&query);
        appendStringInfo(&query,
            "DELETE FROM public._timeseries_workers WHERE db_oid = %u", db_oid);
        SPI_execute(query.data, false, 0);

        elog(LOG, "launcher: extension dropped in db oid=%u, cleaned up", db_oid);
    }
}


// spawn worker in database that attached extension 
static void
launcher_spawn_all_workers(void)
{
    int ret;
    uint64 n;
    Oid *db_oids;

    ret = SPI_execute(
        "SELECT db_oid FROM public._timeseries_workers",
        true, 0);
    
    if(ret != SPI_OK_SELECT){
        elog(WARNING, "launcher: fail to query databases");
        return;
    }

    n = SPI_processed;
    if(n == 0)
        return;
    
    db_oids = palloc(n * sizeof(Oid));
    for(uint64 i=0; i < n; i++){
        bool isnull;
        db_oids[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
    }

    for(uint64 i=0; i < n; i++){
        spawn_worker(db_oids[i]);
    }
}


/*
    Public function
*/

void
launcher_main(Datum main_arg)
{
    pqsignal(SIGTERM, launcher_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    // connect to postgres (default database) to search another database that attached extension
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    pgstat_report_appname("simple_timeseries launcher");

    elog(LOG, "simple_timeseries launcher started");

    while(!got_sigterm){
        WaitLatch(MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            20000L,   // check for every 20 sec.
            PG_WAIT_EXTENSION);
        ResetLatch(MyLatch); // for wait again

        // bgw acknowledge
        CHECK_FOR_INTERRUPTS();

        if(got_sigterm)
            break;

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        PG_TRY();
        {
            launcher_cleanup();
            launcher_spawn_all_workers();
        }
        PG_CATCH();
        {
            EmitErrorReport();
            FlushErrorState();
        }
        PG_END_TRY();
        
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    elog(LOG, "simple_timeseries launcher shutting down");    
}