# TimeSeries Extension
A lightweight PostgreSQL extension that converts regular tables into **hypertables** (special tables optimized for time-series data). It automatically:
- ✅ Partitions data into chunks based on time intervals
- ✅ Routes INSERT operations to appropriate chunks
- ✅ Creates new chunks automatically for new time intervals.

## Key Concepts

`Hypertable`: A regular PostgreSQL table converted to handle time-series data efficiently

`Chunk`: A partition that stores data for a specific time range

```
hypertable
├── chunk1  [2025-01-01 to 2025-01-02)
├── chunk2  [2025-01-02 to 2025-01-03)
└── chunk3  [2025-01-03 to 2025-01-04)
```

## Installation & Build

### Prerequisites packages
```
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    postgresql-server-dev-17 \
    libssl-dev \
    libkrb5-dev
```

### Build and use extension
```
# change to root user
su
./build.sh

# change to postgres user
su - postgres
psql -U postgres

# create database and switch
CREATE DATABASE test_extension;
\c test_extension

# add extension
CREATE EXTENSION simple_timeseries;
```

## Usage Guide
### Creating hypertable
```
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time', INTERVAL '1 day');
```

### Insert hypertable
```
INSERT INTO sensor_data VALUES ('2024-01-01 00:00:00+00', 1, 25.5, 60.0);
```

### Drop hypertable
```
SELECT drop_hypertable('public.sensor_data');
```

### Show all functions
```
\df
```

## Advanced Features
### Background workers
- check background worker inside database
```
SELECT pid, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'test_db';
```
- enable/disable background workers
```
SELECT start_background_workers();

SELECT stop_background_workers();
```

### Retention
- Retention will automatically delete chunks when their duration exceeds the retention policy we set.
```
# set retention policy (automatically delete the chunk that older than 365 days)
SELECT set_retention_policy('sensor_data', INTERVAL '365 days');
```

### Continuous Aggregate
- Continuous aggregation pre-calculates and stores the query results, so when you need to query, you can directly retrieve the results without recalculating, making the query speed very fast.

- create continuous daily aggreagate policy
```
SELECT create_continuous_aggregate(
    'sensor_daily',       -- new daily table name
    'sensor_readings',    -- original table name
    'SELECT               -- aggreagate query
        time_bucket(''1 day'', time) AS bucket,
        AVG(temperature) AS avg_temp,
        MIN(temperature) AS min_temp,
        MAX(temperature) AS max_temp
     FROM sensor_readings
     GROUP BY bucket
     ORDER BY bucket',
    INTERVAL '1 day',      -- Each row stores a query result representing a one-day time interval (the size of the time_bucket)
    INTERVAL '1 hour'      -- auto refresh every 1 hour
);
```

- manual refresh
```
SELECT refresh_continuous_aggregate(
    'sensor_daily',
    '2026-02-12 00:00:00+00'::timestamptz,    
    '2026-02-16 00:00:00+00'::timestamptz
);
```

### Chunk Compression
- check the original chunk size
```
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size(
        quote_ident(schema_name) || '.' || quote_ident(table_name)
    )) AS size,
    (SELECT COUNT(*) FROM public._hyper_1_1_chunk) AS rows
FROM _timeseries_catalog.chunk
WHERE id = 1
ORDER BY table_name;
```

- compress specific chunk
```
SELECT compress_chunk('_hyper_1_1_chunk');
```

- check the compressed size
```
SELECT
    pg_size_pretty(SUM(pg_column_size(column_data)::bigint)) AS compressed_size
FROM _timeseries_catalog.compressed_chunk
WHERE chunk_id = 1;
```

#### NOTE: You can test more extension features on `test` directory

## Compile Locally
```
cd project/
sudo ./build.sh
```

## Cross-platform Deployment
- Local Environment: Ubuntu 24.04 LTS / Postgres 17
- Target Environment: RHEL 9.7 / Postgres 16

### Local machine (Ubuntu)
```
sudo ./build_package.sh
```
<img width="515" height="78" alt="image" src="https://github.com/user-attachments/assets/8f93c9a6-3ccb-42fa-95ad-33e8e897d9d7" />

after compiled, send `timeseries-extension.tar.gz` package to target machine (RHEL)

### Target machine (RHEL)
```
# go to package location and extract package
tar -xzf timeseries-extension.tar.gz

# install extension
sudo ./install.sh

# create extension in PostgreSQL
sudo su - postgres
psql -U postgres
CREATE EXTENSION simple_timeseries;
```
