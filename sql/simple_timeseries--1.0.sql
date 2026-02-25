-- ==========================================
-- utils.c
-- ==========================================

CREATE FUNCTION get_current_timestamp_seconds()
RETURNS bigint
AS 'MODULE_PATHNAME', 'get_current_timestamp_seconds'
LANGUAGE C STRICT;

COMMENT ON FUNCTION get_current_timestamp_seconds() IS 'Get current timestamp in seconds since epoch';

-- ==========================================
-- metadata.c
-- ==========================================

CREATE SCHEMA _timeseries_catalog;

-- restrict all user 
REVOKE ALL ON SCHEMA _timeseries_catalog FROM PUBLIC;

-- hypertable
CREATE TABLE _timeseries_catalog.hypertable (
    id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,                 
    table_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(schema_name, table_name)
);
COMMENT ON TABLE _timeseries_catalog.hypertable IS 
    'Stores metadata about hypertables';

-- dimension
CREATE TABLE _timeseries_catalog.dimension (
    id SERIAL PRIMARY KEY,
    hypertable_id INTEGER NOT NULL REFERENCES _timeseries_catalog.hypertable(id) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    column_type REGTYPE NOT NULL,
    interval_length BIGINT NOT NULL,
    
    UNIQUE(hypertable_id)
);

COMMENT ON TABLE _timeseries_catalog.dimension IS 
    'Stores time dimension information for each hypertable';

-- chunk
CREATE TABLE _timeseries_catalog.chunk (
    id SERIAL PRIMARY KEY,
    hypertable_id INTEGER NOT NULL REFERENCES _timeseries_catalog.hypertable(id) ON DELETE CASCADE,
    schema_name TEXT NOT NULL,                 
    table_name TEXT NOT NULL, -- chunk name
    start_time BIGINT NOT NULL,                
    end_time BIGINT NOT NULL,
    is_compressed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(schema_name, table_name),
    CHECK(end_time > start_time)
);

-- index for finding chunk
CREATE INDEX chunk_hypertable_id_idx 
    ON _timeseries_catalog.chunk(hypertable_id);
CREATE INDEX chunk_time_range_idx 
    ON _timeseries_catalog.chunk(hypertable_id, start_time, end_time);

COMMENT ON TABLE _timeseries_catalog.chunk IS 
    'Stores metadata about chunks (data partitions)';

-- hypertable view
CREATE VIEW _timeseries_catalog.hypertables AS
SELECT
    h.id,
    h.schema_name,
    h.table_name,
    d.column_name AS time_column,
    d.interval_length,
    h.created_at,
    COUNT(c.id) AS num_chunks
FROM _timeseries_catalog.hypertable h
LEFT JOIN _timeseries_catalog.dimension d ON h.id = d.hypertable_id
LEFT JOIN _timeseries_catalog.chunk c ON h.id = c.hypertable_id
GROUP BY h.id, h.schema_name, h.table_name, d.column_name, d.interval_length, h.created_at;


-- ==========================================
-- FUNCTION
-- ==========================================

-- check whether hypertable exists
CREATE FUNCTION check_hypertable_exists(
    schema_name text,
    table_name text
) RETURNS boolean AS $$
BEGIN
    RETURN EXISTS (
        SELECT *
        FROM _timeseries_catalog.hypertable h
        WHERE h.schema_name = check_hypertable_exists.schema_name 
            AND h.table_name = check_hypertable_exists.table_name
    );
END;
$$ LANGUAGE plpgsql;


-- get hypertable info
CREATE FUNCTION get_hypertable_info(
    s_name text,
    t_name text
) RETURNS text AS $$
DECLARE
    info_text text;
BEGIN
    SELECT format(
        'ID: %s | Schema: %s | Table: %s | Time Column: %s | Interval: %s | Chunks: %s | Created: %s',
        id, schema_name, table_name, time_column, interval_length, num_chunks, created_at) INTO info_text
    FROM _timeseries_catalog.hypertables
    WHERE schema_name = s_name AND table_name = t_name;

    IF info_text IS NULL THEN
        RETURN format('Hypertable %s.%s not found', s_name, t_name);
    END IF;

    RETURN info_text;
END;
$$ LANGUAGE plpgsql;


-- display chunk by hypertable id
CREATE FUNCTION get_chunks_by_hid(
    p_hypertable_id int
) RETURNS TABLE (table_name text, start_time timestamptz, end_time timestamptz) AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        c.table_name,
        TO_TIMESTAMP((c.start_time::double precision / 1000000) + 946684800) as start_time,
        TO_TIMESTAMP((c.end_time::double precision / 1000000) + 946684800) as end_time
    FROM _timeseries_catalog.chunk c
    WHERE c.hypertable_id = p_hypertable_id
    ORDER BY c.start_time;
END;
$$ LANGUAGE plpgsql;


-- display all chunks
CREATE FUNCTION display_all_chunks(
) RETURNS TABLE (schema_name name, table_name name) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pg.schemaname as schema_name,
        pg.tablename as table_name
    FROM pg_tables as pg
    WHERE pg.tablename LIKE '_hyper_%';
END;
$$ LANGUAGE plpgsql;

-- show all triggers
CREATE FUNCTION display_all_triggers(    
) RETURNS TABLE (table_name name, trigger_name name) AS $$
BEGIN
    RETURN QUERY
    SELECT
        info.event_object_table::name as table_name,
        info.trigger_name::name as trigger_name
    FROM
        information_schema.triggers as info
    ORDER BY
        info.event_object_table,
        info.trigger_name;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- HYPERTABLE FUNCTIONS
-- ==========================================

-- create hypertable
CREATE FUNCTION create_hypertable(
    table_name REGCLASS,
    time_column_name TEXT,
    chunk_time_interval INTERVAL
) RETURNS VOID
AS 'MODULE_PATHNAME', 'create_hypertable'
LANGUAGE C STRICT;

-- drop hypertable
CREATE FUNCTION drop_hypertable(
    table_name REGCLASS
) RETURNS VOID
AS 'MODULE_PATHNAME', 'drop_hypertable'
LANGUAGE C STRICT;

-- ==========================================
-- TRIGGER FUNCTIONS
-- ==========================================
CREATE FUNCTION trigger_insert()
RETURNS TRIGGER
AS 'MODULE_PATHNAME', 'trigger_insert'
LANGUAGE C;

-- ==========================================
-- DATA RETENTION SYSTEM
-- ==========================================

-- store data retention policy table
CREATE TABLE _timeseries_catalog.retention_policies (
    hypertable_id INTEGER NOT NULL REFERENCES _timeseries_catalog.hypertable(id) ON DELETE CASCADE,
    retain_microseconds BIGINT NOT NULL CHECK (retain_microseconds > 0),
    retain_periods INTERVAL NOT NULL CHECK (retain_periods > INTERVAL '0'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(hypertable_id)
);

-- drop chunk
CREATE FUNCTION drop_chunks(
    hypertable   REGCLASS,
    older_than   INTERVAL
) RETURNS INTEGER
AS 'MODULE_PATHNAME', 'drop_chunks'
LANGUAGE C STRICT;

-- set retention policy
CREATE FUNCTION set_retention_policy(
    hypertable        REGCLASS,
    retention_period  INTERVAL
) RETURNS VOID
AS 'MODULE_PATHNAME', 'set_retention_policy'
LANGUAGE C STRICT;

-- remove policy
CREATE FUNCTION remove_retention_policy(
    hypertable  REGCLASS
) RETURNS VOID
AS 'MODULE_PATHNAME', 'remove_retention_policy'
LANGUAGE C STRICT;

-- apply to all policy (manual)
CREATE FUNCTION apply_retention_policies()
RETURNS VOID
AS 'MODULE_PATHNAME', 'apply_retention_policies'
LANGUAGE C STRICT;

-- start retention background worker for current database
CREATE FUNCTION start_retention_worker()
RETURNS VOID
AS 'MODULE_PATHNAME', 'start_retention_worker'
LANGUAGE C STRICT;
 
SELECT start_retention_worker(); 

-- ==========================================
-- CONTINUOUS AGGREGATES
-- ==========================================

-- continuous aggregate catalog
CREATE TABLE _timeseries_catalog.continuous_aggregate (
    id                SERIAL PRIMARY KEY,
    view_name         TEXT NOT NULL UNIQUE,
    hypertable_id     INTEGER NOT NULL
                          REFERENCES _timeseries_catalog.hypertable(id) ON DELETE CASCADE,
    view_definition   TEXT NOT NULL,
    bucket_width      BIGINT NOT NULL,
    refresh_interval  BIGINT DEFAULT NULL,  -- microseconds, NULL = manual only
    watermark         BIGINT NOT NULL DEFAULT 0,  -- last refreshed timestamp
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ
);

-- round timestamp for place in the same bucket
CREATE FUNCTION time_bucket(
    bucket_width  INTERVAL,
    ts            TIMESTAMPTZ
) RETURNS TIMESTAMPTZ
AS 'MODULE_PATHNAME', 'time_bucket'
LANGUAGE C STRICT;

-- create continuous aggregate
CREATE FUNCTION create_continuous_aggregate(
    view_name         TEXT,
    hypertable        REGCLASS,
    view_sql          TEXT,
    bucket_width      INTERVAL,
    refresh_interval  INTERVAL DEFAULT NULL
) RETURNS VOID
AS 'MODULE_PATHNAME', 'create_continuous_aggregate'
LANGUAGE C;

-- refresh cagg (range)
CREATE FUNCTION refresh_continuous_aggregate(
    view_name   TEXT,
    start_time  TIMESTAMPTZ,
    end_time    TIMESTAMPTZ
) RETURNS VOID
AS 'MODULE_PATHNAME', 'refresh_continuous_aggregate'
LANGUAGE C STRICT;

-- drop continuous aggregate
CREATE FUNCTION drop_continuous_aggregate(
    view_name  TEXT
) RETURNS VOID
AS 'MODULE_PATHNAME', 'drop_continuous_aggregate'
LANGUAGE C STRICT;

-- start continuous aggregate worker for current database
CREATE FUNCTION start_cagg_worker()
RETURNS VOID
AS 'MODULE_PATHNAME', 'start_cagg_worker'
LANGUAGE C STRICT;
 
SELECT start_cagg_worker(); 

-- ==========================================
-- COMPRESSION SYSTEM
-- ==========================================

-- compressed chunk storage
CREATE TABLE _timeseries_catalog.compressed_chunk (
    id                 SERIAL PRIMARY KEY,
    chunk_id           INTEGER NOT NULL REFERENCES _timeseries_catalog.chunk(id) ON DELETE CASCADE,
    column_name        TEXT NOT NULL,
    column_type        TEXT NOT NULL,
    column_data        BYTEA NOT NULL,     -- TOAST compression happens here
    row_count          INTEGER,
    uncompressed_bytes BIGINT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX compressed_chunk_chunk_id_idx ON _timeseries_catalog.compressed_chunk(chunk_id);

-- compress chunk
CREATE FUNCTION compress_chunk(
    chunk_name  REGCLASS
) RETURNS VOID
AS 'MODULE_PATHNAME', 'compress_chunk'
LANGUAGE C STRICT;

-- check compressed table
SELECT
    chunk_id,
    column_name,
    column_type,
    row_count                                            
FROM _timeseries_catalog.compressed_chunk
WHERE chunk_id = 1
ORDER BY id;


-- ==========================================
-- BACKGROUND WORKER
-- ==========================================

-- stop background workers
CREATE FUNCTION stop_background_workers(
) RETURNS event_trigger AS $$
BEGIN
    PERFORM pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE (application_name IN ('retention worker', 'continuous aggregate worker'))
        AND (datname = current_database());
END;
$$ LANGUAGE plpgsql;

-- event trigger drop bgw when called DROP EXTENSION
CREATE EVENT TRIGGER drop_bgw_when_drop_extension
ON ddl_command_start
WHEN TAG IN ('DROP EXTENSION')
EXECUTE FUNCTION stop_background_workers();