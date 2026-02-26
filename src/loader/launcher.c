#include <postgres.h>
#include <postmaster/bgworker.h>
#include <storage/latch.h>
#include <executor/spi.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <utils/hsearch.h>

static volatile sig_atomic_t got_sigterm = false;

static void
launcher_sigterm_handler(SIGNAL_ARGS)
{
    got_sigterm = true; // received sigterm
    SetLatch(MyLatch); // wake worker up
}

// database status
typedef enum {
    DB_STATE_UNKNOWN,        // unknown extension existing 
    DB_STATE_NO_EXTENSION,   // no spawn
    DB_STATE_RUNNING         // already spawn
} DbState;


// database entry
typedef struct DbEntry {
    Oid db_oid;
    DbState state;
    bool retention_running;
    bool cagg_running;
    BackgroundWorkerHandle *retention_handle;
    BackgroundWorkerHandle *cagg_handle;
} DbEntry;

// hash table for store each database status
static HTAB *db_htab = NULL; 

static void 
init_db_htab(void)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(DbEntry);
    db_htab = hash_create("DB Worker Table", 32, &ctl, HASH_ELEM | HASH_BLOBS);
}

static void
spawn_workers_for_db(Oid db_oid)
{
    DbEntry *entry;
    bool found;
    BgwHandleStatus status;
    pid_t pid;

    entry = (DbEntry *) hash_search(db_htab, &db_oid, HASH_ENTER, &found);
    if(!found){
        entry->db_oid = db_oid;
        entry->state = DB_STATE_UNKNOWN;
        entry->retention_handle = NULL;
        entry->cagg_handle = NULL;
        entry->retention_running = false;
        entry->cagg_running = false;
    }

    // no extension: skip
    if(entry->state == DB_STATE_NO_EXTENSION)
        return;

    // check worker handle
    if(entry->retention_handle != NULL){
        status = GetBackgroundWorkerPid(entry->retention_handle, &pid);
        if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
            entry->retention_running = false;
            entry->state = DB_STATE_UNKNOWN;
    }

    if(entry->cagg_handle != NULL){
        status = GetBackgroundWorkerPid(entry->cagg_handle, &pid);
        if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
            entry->cagg_running = false;
            entry->state = DB_STATE_UNKNOWN;
    }


    // spawn worker
    if(!entry->retention_running){
        BackgroundWorker worker;
        
        MemSet(&worker, 0, sizeof(worker));
        strlcpy(worker.bgw_name, "retention worker", BGW_MAXLEN);
        strlcpy(worker.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
        strlcpy(worker.bgw_function_name, "retention_worker_main", BGW_MAXLEN);
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = BGW_NEVER_RESTART;
        worker.bgw_main_arg = ObjectIdGetDatum(db_oid);
        worker.bgw_notify_pid = MyProcPid;

        if (RegisterDynamicBackgroundWorker(&worker, &entry->retention_handle)){
            entry->retention_running = true;
            entry->state = DB_STATE_RUNNING;
        }
    }

    if(!entry->cagg_running){
        BackgroundWorker worker;

        MemSet(&worker, 0, sizeof(worker));
        strlcpy(worker.bgw_name, "continuous aggregate worker", BGW_MAXLEN);
        strlcpy(worker.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
        strlcpy(worker.bgw_function_name, "cagg_worker_main", BGW_MAXLEN);
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = BGW_NEVER_RESTART;
        worker.bgw_main_arg = ObjectIdGetDatum(db_oid);
        worker.bgw_notify_pid = MyProcPid;

        if (RegisterDynamicBackgroundWorker(&worker, &entry->cagg_handle)){
            entry->cagg_running = true;
            entry->state = DB_STATE_RUNNING;
        }
    }
}

static void
scan_and_spawn_all_dbs(void)
{
    int ret;
    StringInfoData query;

    SetCurrentStatementStartTimestamp();

    StartTransactionCommand();
    SPI_connect();
    
    // save database snapshot into stack
    PushActiveSnapshot(GetTransactionSnapshot());

    // search database that attached extension
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT oid FROM pg_database "
        "WHERE datallowconn = true "
        "   AND datname NOT IN ('template0', 'template1', 'postgres')");
    ret = SPI_execute(query.data, true, 0);
    
    if(ret == SPI_OK_SELECT && SPI_processed > 0){
        for(uint64 i = 0; i < SPI_processed; i++){
            bool isnull;
            Oid db_oid = DatumGetObjectId(
                            SPI_getbinval(SPI_tuptable->vals[i],SPI_tuptable->tupdesc, 1, &isnull));
            if (!isnull)
                spawn_workers_for_db(db_oid);
        }
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

void 
launcher_main(Datum main_arg)
{ 
    pqsignal(SIGTERM, launcher_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    // set launcher to postgres db for scan pg_database
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    pgstat_report_appname("timeseries launcher");

    init_db_htab();
    elog(LOG, "timeseries launcher started");

    while(!got_sigterm){
        scan_and_spawn_all_dbs();

        WaitLatch(MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                10000L,  // check database for every 10 sec
                PG_WAIT_EXTENSION);
        
        ResetLatch(MyLatch);
    }

    elog(LOG, "timeseries launcher shutting down");
}
