#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>
#include <access/xact.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <miscadmin.h>
#include <funcapi.h>

#include "metadata.h"
#include "retention.h"

#define USECS_PER_DAY INT64CONST(86400000000)

// postgresql use SIGTERM as the signal for background worker to stop
// when postgresql shutdown, it will send SIGTERM to every background workers
static volatile sig_atomic_t got_sigterm = false;

static void
retention_sigterm_handler(SIGNAL_ARGS)
{
    got_sigterm = true; // received sigterm
    SetLatch(MyLatch); // wake worker up
}

int 
retention_drop_old_chunks(int hypertable_id, int64 cutoff_time)
{
    StringInfoData query;
    int ret, dropped = 0;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id, schema_name, table_name "
        "FROM _timeseries_catalog.chunk "
        "WHERE hypertable_id = %d AND end_time <= " INT64_FORMAT
        " ORDER BY start_time",
        hypertable_id, cutoff_time);

    ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT)
        ereport(ERROR, (errmsg("retention: failed to query old chunks")));

    if (SPI_processed == 0)
        return 0;

    // store result, since drop every iterration it called spi_execute, it will overwrite SPI_tuptable and SPI_processed
    typedef struct { 
        int id; 
        char schema[NAMEDATALEN]; 
        char table[NAMEDATALEN]; 
    } ChunkRow;

    ChunkRow *rows = (ChunkRow *) palloc(SPI_processed * sizeof(ChunkRow));
    int64 n = SPI_processed;

    for(uint64 i=0; i < n; i++){
        bool isnull;
        Datum datum;
        rows[i].id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));

        datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);
        strlcpy(rows[i].schema, TextDatumGetCString(datum), NAMEDATALEN);

        datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull);
        strlcpy(rows[i].table,  TextDatumGetCString(datum), NAMEDATALEN);
    }

    // drop chunk
    for(uint64 i=0; i < n; i++){
        StringInfoData drop_query;
        initStringInfo(&drop_query);
        appendStringInfo(&drop_query,
            "DROP TABLE IF EXISTS %s.%s",
            quote_identifier(rows[i].schema),
            quote_identifier(rows[i].table));

        ret = SPI_execute(drop_query.data, false, 0);
        if (ret != SPI_OK_UTILITY){
            elog(WARNING, "retention: failed to drop chunk %s.%s", rows[i].schema, rows[i].table);
            continue;
        }

        // remove metadata
        resetStringInfo(&drop_query);
        appendStringInfo(&drop_query,
            "DELETE FROM _timeseries_catalog.chunk WHERE id = %d", rows[i].id);    
        SPI_execute(drop_query.data, false, 0);
        elog(NOTICE, "retention: dropped chunk %s.%s ", rows[i].schema, rows[i].table);
        
        dropped++;
    }

    return dropped;
}

void
retention_set_policy(int hypertable_id, int64 retain_microseconds, char *retain_days)
{
    StringInfoData query;
    int ret;
    initStringInfo(&query);
    appendStringInfo(&query,
                    "INSERT INTO _timeseries_catalog.retention_policies "
                    "(hypertable_id, retain_microseconds, retain_periods) "
                    "VALUES (%d, " INT64_FORMAT ", INTERVAL '%s') "
                    "ON CONFLICT (hypertable_id) DO UPDATE "
                    "    SET retain_microseconds = EXCLUDED.retain_microseconds, "
                    "        updated_at = NOW()",
                    hypertable_id, retain_microseconds, retain_days);

    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_INSERT && ret != SPI_OK_UPDATE)
        ereport(ERROR, (errmsg("retention: failed to set policy")));
}

void 
retention_drop_policy(int hypertable_id)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "DELETE FROM _timeseries_catalog.retention_policies "
        "WHERE hypertable_id = %d", hypertable_id);
    SPI_execute(query.data, false, 0);
}

int 
retention_apply_all_policies(void)
{
    StringInfoData query;
    int ret, total = 0;
    uint64 i, n;
    int64 current_time = GetCurrentTimestamp();

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT hypertable_id, retain_microseconds "
        "FROM _timeseries_catalog.retention_policies");
    
    ret = SPI_execute(query.data, true, 0);
    if(ret != SPI_OK_SELECT || SPI_processed == 0)
        return 0;
    
    typedef struct {
        int hypertable_id; 
        int64 retain_microseconds;
    } PolicyRow;

    PolicyRow *rows = (PolicyRow *) palloc(SPI_processed * sizeof(PolicyRow));
    n = SPI_processed;

    for(i=0; i < n; i++){
        bool isnull;
        rows[i].hypertable_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
        rows[i].retain_microseconds = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
    }

    for (i=0; i < n; i++){
        int64 cutoff_time = current_time - rows[i].retain_microseconds;
        int dropped = retention_drop_old_chunks(rows[i].hypertable_id, cutoff_time);
        total += dropped;
        if (dropped > 0)
            elog(NOTICE, "retention: hypertable %d : dropped %d chunk(s)", rows[i].hypertable_id, dropped);
    }

    return total;
}

void 
retention_worker_main(Datum main_arg)
{
    // register signal handler
    pqsignal(SIGTERM, retention_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection("test_db", NULL, 0);

    while(!got_sigterm){
        int ret;

        // wait 60 sec for receive signal
        ret = WaitLatch(MyLatch,
                        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                        60000L,  // 60 seconds
                        PG_WAIT_EXTENSION);
        
        ResetLatch(MyLatch);

        if(got_sigterm) 
            break;

        if(ret & WL_TIMEOUT){
            // apply policy
            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());

            int dropped = retention_apply_all_policies();
            if (dropped > 0)
                elog(LOG, "retention worker: total %d chunk(s) dropped", dropped);

            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
        }
    }

    elog(LOG, "retention worker shutting down");
}

PG_FUNCTION_INFO_V1(drop_chunks);
Datum
drop_chunks(PG_FUNCTION_ARGS)
{
    Oid table_oid = PG_GETARG_OID(0);
    Interval *older_than = PG_GETARG_INTERVAL_P(1);

    char *schema_name = get_namespace_name(get_rel_namespace(table_oid));
    char *table_name  = get_rel_name(table_oid);

    // calculate cutoff time (current time - older_than(aka.duration))
    /* eg.
            current time = 14:00
            older_than = 1 hrs
            cutoff = 13:00
        Which mean the chunk that older than 13:00 (12:59, 11:30), will get drop
    */
    TimestampTz current_time = GetCurrentTimestamp();
    TimestampTz cutoff_time = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_mi_interval,
                                                    TimestampTzGetDatum(current_time),
                                                    PointerGetDatum(older_than)));
    
    SPI_connect();

    int hypertable_id = metadata_get_hypertable_id(schema_name, table_name);
    if(hypertable_id == -1){
        SPI_finish();
        ereport(ERROR, (errmsg("table \"%s.%s\" is not a hypertable", schema_name, table_name)));
    }

    int dropped = retention_drop_old_chunks(hypertable_id, (int64) cutoff_time);
    elog(NOTICE, "drop_chunks: removed %d chunk(s) older than %s from \"%s.%s\"",
        dropped,
        DatumGetCString(DirectFunctionCall1(interval_out, PointerGetDatum(older_than))),
        schema_name, table_name);
    
    SPI_finish();
    PG_RETURN_INT32(dropped);
}

PG_FUNCTION_INFO_V1(set_retention_policy);
Datum
set_retention_policy(PG_FUNCTION_ARGS)
{
    Oid table_oid = PG_GETARG_OID(0);
    Interval *retain_periods = PG_GETARG_INTERVAL_P(1);

    char *schema_name = get_namespace_name(get_rel_namespace(table_oid));
    char *table_name  = get_rel_name(table_oid);
    char *retain_days = DatumGetCString(DirectFunctionCall1(interval_out, PointerGetDatum(retain_periods)));

    // convert interval to microsecond
    int64 retain_microseconds = (int64) retain_periods->day  * USECS_PER_DAY + (int64) retain_periods->time;

    SPI_connect();

    int hypertable_id = metadata_get_hypertable_id(schema_name, table_name);
    if(hypertable_id == -1){
        SPI_finish();
        ereport(ERROR, (errmsg("table \"%s.%s\" is not a hypertable", schema_name, table_name)));
    }

    retention_set_policy(hypertable_id, retain_microseconds, retain_days);

    elog(NOTICE, "set_retention_policy: \"%s.%s\" will retain data for %s",
        schema_name, table_name, retain_days);

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(remove_retention_policy);
Datum
remove_retention_policy(PG_FUNCTION_ARGS)
{
    Oid table_oid = PG_GETARG_OID(0);

    char *schema_name = get_namespace_name(get_rel_namespace(table_oid));
    char *table_name = get_rel_name(table_oid);

    SPI_connect();

    int hypertable_id = metadata_get_hypertable_id(schema_name, table_name);
    if (hypertable_id == -1){
        SPI_finish();
        ereport(ERROR, (errmsg("table \"%s.%s\" is not a hypertable", schema_name, table_name)));
    }

    retention_drop_policy(hypertable_id);
    elog(NOTICE, "remove_retention_policy: policy removed from \"%s.%s\"", schema_name, table_name);

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(apply_retention_policies);
Datum
apply_retention_policies(PG_FUNCTION_ARGS)
{
    SPI_connect();
    int total = retention_apply_all_policies();
    SPI_finish();

    elog(NOTICE, "apply_retention_policies: %d chunk(s) dropped in total", total);
    PG_RETURN_VOID();
}

