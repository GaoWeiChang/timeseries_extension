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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    compressed BOOLEAN DEFAULT false,
    compressed_at TIMESTAMPTZ,
    
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
) RETURNS boolean 
AS $$
BEGIN
    RETURN EXISTS (
        SELECT *
        FROM _timeseries_catalog.hypertable h
        WHERE h.schema_name = check_hypertable_exists.schema_name 
            AND h.table_name = check_hypertable_exists.table_name
    );
END;
$$ 
LANGUAGE plpgsql;


-- get hypertable info
CREATE FUNCTION get_hypertable_info(
    s_name text,
    t_name text
) RETURNS text 
AS $$
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
$$ 
LANGUAGE plpgsql;


-- display chunk by hypertable id
CREATE FUNCTION get_chunks_by_hid(
    p_hypertable_id int
) RETURNS TABLE (table_name text, start_time timestamptz, end_time timestamptz)
AS $$
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
$$ 
LANGUAGE plpgsql;


-- display all chunks
CREATE FUNCTION display_all_chunks(
) RETURNS TABLE (schema_name name, table_name name)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pg.schemaname as schema_name,
        pg.tablename as table_name
    FROM pg_tables as pg
    WHERE pg.tablename LIKE '_hyper_%'
    ORDER BY pg.tablename;
END;
$$ 
LANGUAGE plpgsql;

-- show all triggers
CREATE FUNCTION display_all_triggers(    
) RETURNS TABLE (table_name name, trigger_name name)
AS $$
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
$$ 
LANGUAGE plpgsql;

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
-- COMPRESSION FUNCTIONS
-- ==========================================
-- ALTER TABLE _timeseries_catalog.chunk 
-- ADD COLUMN IF NOT EXISTS compressed BOOLEAN DEFAULT false,
-- ADD COLUMN IF NOT EXISTS compressed_at TIMESTAMPTZ;

-- compress specific chunk
CREATE FUNCTION compress_chunk(
    chunk_id INTEGER
)
RETURNS BOOLEAN
AS 'MODULE_PATHNAME', 'compress_chunk'
LANGUAGE C STRICT;

-- decompress specific chunk
CREATE FUNCTION decompress_chunk(
    chunk_id INTEGER
)
RETURNS BOOLEAN
AS 'MODULE_PATHNAME', 'decompress_chunk'
LANGUAGE C STRICT;

-- compress chunk that older than interval
CREATE FUNCTION compress_chunks_older_than(
    table_name TEXT,
    older_than INTERVAL
)
RETURNS INTEGER
AS 'MODULE_PATHNAME', 'compress_chunks_older_than'
LANGUAGE C STRICT;

-- Show compression statistics
CREATE FUNCTION show_chunk_compression_stats(
    table_name TEXT
)
RETURNS TEXT
AS 'MODULE_PATHNAME', 'show_chunk_compression_stats'
LANGUAGE C STRICT;