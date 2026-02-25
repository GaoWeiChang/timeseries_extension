#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>
#include <access/xact.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <funcapi.h>

#include "../../src/metadata.h"
#include "continuous_aggs.h"

// bgw signal handler
static volatile sig_atomic_t got_sigterm = false;

static void
cagg_sigterm_handler(SIGNAL_ARGS)
{
    got_sigterm = true; // received sigterm
    SetLatch(MyLatch); // wake worker up
}

// update watermark
void 
cagg_set_watermark(int cagg_id, int64 watermark)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "UPDATE _timeseries_catalog.continuous_aggregate "
        "SET watermark = " INT64_FORMAT ", updated_at = NOW() "
        "WHERE id = %d", 
        watermark, cagg_id);

    SPI_execute(query.data, false, 0);
}

// create continuous aggregate
void 
cagg_create(const char *cagg_name,
           const char *hypertable_schema,
           const char *hypertable_name,
           const char *view_sql,
           int64 bucket_width)
{
    StringInfoData query;
    int hypertable_id, ret;

    hypertable_id = metadata_get_hypertable_id(hypertable_schema, hypertable_name);
    if (hypertable_id == -1)
        ereport(ERROR, (errmsg("table \"%s.%s\" is not a hypertable", hypertable_schema, hypertable_name)));

    // create materialized table
    initStringInfo(&query);
    appendStringInfo(&query,
        "CREATE TABLE _timeseries_catalog.%s AS %s WITH NO DATA",
        quote_identifier(cagg_name), view_sql);

    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY)
        ereport(ERROR, (errmsg("failed to create materialized table for cagg \"%s\"", cagg_name)));
    
    // save metadata
    resetStringInfo(&query);
    appendStringInfo(&query,
        "INSERT INTO _timeseries_catalog.continuous_aggregate (view_name, hypertable_id, view_definition, bucket_width, watermark) "
        "VALUES ('%s', %d, %s, " INT64_FORMAT ", 0)",
        cagg_name, hypertable_id, quote_literal_cstr(view_sql), bucket_width);

    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_INSERT)
        ereport(ERROR, (errmsg("failed to insert cagg metadata")));

    elog(NOTICE, "continuous aggregate \"%s\" created", cagg_name);
}

// update data in continuous aggregate
void 
cagg_refresh(int cagg_id, int64 start_time, int64 end_time)
{
    StringInfoData query;
    int ret;
    char *view_name, *view_def;
    bool isnull;
    Datum datum;

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT view_name, view_definition "
        "FROM _timeseries_catalog.continuous_aggregate "
        "WHERE id = %d", cagg_id);
    
    ret = SPI_execute(query.data, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        ereport(ERROR, (errmsg("continuous aggregate id %d not found", cagg_id)));
    
    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    view_name = pstrdup(TextDatumGetCString(datum));

    datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
    view_def  = pstrdup(TextDatumGetCString(datum));
    
    // remove old data in range
    resetStringInfo(&query);
    appendStringInfo(&query,
        "DELETE FROM _timeseries_catalog.%s "
        "WHERE bucket >= '2000-01-01 UTC'::timestamptz + '%ld microseconds'::interval "
        "AND bucket < '2000-01-01 UTC'::timestamptz + '%ld microseconds'::interval",
        quote_identifier(view_name), start_time, end_time);

    SPI_execute(query.data, false, 0);

    // recompute and insert new data
    resetStringInfo(&query);
    appendStringInfo(&query,
        "INSERT INTO _timeseries_catalog.%s "
        "SELECT * FROM (%s) sub "
        "WHERE bucket >= '2000-01-01 UTC'::timestamptz + '%ld microseconds'::interval "
        "   AND bucket < '2000-01-01 UTC'::timestamptz + '%ld microseconds'::interval",
        quote_identifier(view_name), view_def, start_time, end_time);
    
    ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_INSERT)
        ereport(ERROR, (errmsg("failed to refresh cagg \"%s\"", view_name)));

    // update watermark
    cagg_set_watermark(cagg_id, end_time);

    elog(NOTICE, "continuous aggregate \"%s\" refreshed: [%ld, %ld)", view_name, start_time, end_time);
}

// refresh all continuous aggregation that need to refresh
int 
cagg_refresh_all_due(void)
{
    StringInfoData query;
    int ret, refreshed = 0;
    uint64 n, i;
    TimestampTz now = GetCurrentTimestamp();

    // get caggs that need to refresh
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id, view_name, watermark, bucket_width, refresh_interval "
        "FROM _timeseries_catalog.continuous_aggregate "
        "WHERE (refresh_interval > 0) AND "
            "((updated_at IS NULL) OR (NOW() >= (updated_at + CONCAT(refresh_interval, ' microseconds')::interval)))");
    
    ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        return 0;
    
    typedef struct {
        int id;
        char name[NAMEDATALEN];
        int64 watermark;
        int64 bucket_width;
        int64 refresh_interval;
    } CaggRow;

    n = SPI_processed;
    CaggRow *rows = (CaggRow *) palloc(n * sizeof(CaggRow));

    for(i=0; i<n; i++){
        bool isnull;
        Datum datum;

        rows[i].id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));

        datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);
        strlcpy(rows[i].name, TextDatumGetCString(datum), NAMEDATALEN);

        rows[i].watermark = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull));
        rows[i].bucket_width = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull));
        rows[i].refresh_interval = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 5, &isnull));
    }

    // refresh each cagg
    for(i=0; i<n; i++){
        int64 watermark = rows[i].watermark;
        int64 end = (int64) now - rows[i].bucket_width; // not include current bucket since it has not complete data
        if(end > watermark){
            cagg_refresh(rows[i].id, watermark, end);
            elog(NOTICE, "continuous aggregation \"%s\" auto-refreshed", rows[i].name);
            refreshed++;
        }
    }

    return refreshed;
}

// background worker
void 
cagg_worker_main(Datum main_arg)
{
    Oid db_oid = DatumGetObjectId(main_arg);

    // register signal handler
    pqsignal(SIGTERM, cagg_sigterm_handler);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnectionByOid(db_oid, InvalidOid, 0);
    pgstat_report_appname("continuous aggregate worker");
    
    while(!got_sigterm){
        // wait 120 sec for receive signal
        int ret = WaitLatch(MyLatch,
                        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                        120000L,  // 120 seconds
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

            int refreshed = cagg_refresh_all_due();
            if (refreshed > 0)
                elog(LOG, "continuous aggregate worker: refreshed %d continuous aggregate(s)", refreshed);

            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
        }
    }

    elog(LOG, "cagg worker shutting down");
}


// round timestamp for place in the same bucket
PG_FUNCTION_INFO_V1(time_bucket);
Datum 
time_bucket(PG_FUNCTION_ARGS)
{
    Interval *bucket_width = PG_GETARG_INTERVAL_P(0);
    TimestampTz ts = PG_GETARG_TIMESTAMPTZ(1);

    // convert to microsec
    int64 bucket_microseconds = (int64) bucket_width->day  * MICROSECS_PER_DAY + (int64) bucket_width->time;
    int64 ts_microseconds = (int64) ts;

    int64 bucketed_microseconds = (ts_microseconds / bucket_microseconds) * bucket_microseconds;

    PG_RETURN_TIMESTAMPTZ((TimestampTz) bucketed_microseconds);
}


PG_FUNCTION_INFO_V1(create_continuous_aggregate);
Datum 
create_continuous_aggregate(PG_FUNCTION_ARGS)
{
    text *view_name_text = PG_GETARG_TEXT_PP(0);
    Oid hypertable_oid = PG_GETARG_OID(1);
    text *view_sql_text = PG_GETARG_TEXT_PP(2);
    Interval *bucket_width = PG_GETARG_INTERVAL_P(3);
    Interval *refresh_interval = PG_ARGISNULL(4) ? NULL : PG_GETARG_INTERVAL_P(4); // refresh for every time that we set

    char *view_name = text_to_cstring(view_name_text);
    char *view_sql = text_to_cstring(view_sql_text);
    char *schema_name = get_namespace_name(get_rel_namespace(hypertable_oid));
    char *table_name = get_rel_name(hypertable_oid);

    int64 bucket_microseconds = (int64) bucket_width->day  * MICROSECS_PER_DAY + (int64) bucket_width->time;

    SPI_connect();

    cagg_create(view_name, schema_name, table_name, view_sql, bucket_microseconds);

    // set refresh interval
    if(refresh_interval != NULL){
        int64 refresh_microsec = (int64) refresh_interval->day * MICROSECS_PER_DAY + (int64) refresh_interval->time;

        StringInfoData query;
        initStringInfo(&query);
        appendStringInfo(&query,
            "UPDATE _timeseries_catalog.continuous_aggregate "
            "SET refresh_interval = " INT64_FORMAT " "
            "WHERE view_name = '%s'",
            refresh_microsec, view_name);
        SPI_execute(query.data, false, 0);

        elog(NOTICE, "auto-refresh enabled: every %s ",
            DatumGetCString(DirectFunctionCall1(interval_out, PointerGetDatum(refresh_interval))));
    }

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(refresh_continuous_aggregate);
Datum
refresh_continuous_aggregate(PG_FUNCTION_ARGS)
{
    text *view_name_text = PG_GETARG_TEXT_PP(0);
    TimestampTz start_time = PG_GETARG_TIMESTAMPTZ(1);
    TimestampTz end_time = PG_GETARG_TIMESTAMPTZ(2);

    if(start_time > end_time)
        ereport(ERROR, (errmsg("start time must be less than end time.")));

    char *view_name = text_to_cstring(view_name_text);
    int cagg_id, ret;
    StringInfoData query;

    SPI_connect();

    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT id "
        "FROM _timeseries_catalog.continuous_aggregate "
        "WHERE view_name = '%s'",
        view_name);
    
    ret = SPI_execute(query.data, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0){
        SPI_finish();
        ereport(ERROR, (errmsg("continuous aggregate \"%s\" not found", view_name)));
    }

    bool isnull;
    cagg_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

    // refresh
    cagg_refresh(cagg_id, (int64) start_time, (int64) end_time);
    elog(NOTICE, "continuous aggregate \"%s\" refreshed", view_name);

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(drop_continuous_aggregate);
Datum
drop_continuous_aggregate(PG_FUNCTION_ARGS)
{
    text *view_name_text = PG_GETARG_TEXT_PP(0);
    char *view_name = text_to_cstring(view_name_text);
    StringInfoData query;

    SPI_connect();

    // drop materialized table
    initStringInfo(&query);
    appendStringInfo(&query,
        "DROP TABLE IF EXISTS _timeseries_catalog.%s",
        quote_identifier(view_name));
    SPI_execute(query.data, false, 0);
    
    // remove metadata
    resetStringInfo(&query);
    appendStringInfo(&query,
        "DELETE FROM _timeseries_catalog.continuous_aggregate "
        "WHERE view_name = '%s'",
        view_name);
    SPI_execute(query.data, false, 0);
    
    elog(NOTICE, "continuous aggregate \"%s\" dropped", view_name);

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(start_cagg_worker);
Datum 
start_cagg_worker(PG_FUNCTION_ARGS)
{
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;

    MemSet(&worker, 0, sizeof(worker));
    strlcpy(worker.bgw_name, "continuous aggregate worker", BGW_MAXLEN);
    strlcpy(worker.bgw_library_name, "simple_timeseries", BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "cagg_worker_main", BGW_MAXLEN);

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;

    worker.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId);

    RegisterDynamicBackgroundWorker(&worker, &handle);

    PG_RETURN_VOID();
}