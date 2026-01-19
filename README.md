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
├── chunk1  [2024-01-01 to 2024-01-02)
├── chunk2  [2024-01-02 to 2024-01-03)
└── chunk3  [2024-01-03 to 2024-01-04)
```

## Installation & Build

### Prerequisites packages
```
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    postgresql-server-dev-16 \
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
INSERT INTO sensor_data VALUES ('2024-01-01 00:00:00+00'::timestamptz, 1, 25.5, 60.0);
```

### Drop hypertable
```
SELECT drop_hypertable('public.sensor_data');
```

### Show all functions
```
\df
```
