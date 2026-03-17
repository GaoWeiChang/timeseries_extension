#pragma once

#include <postgres.h>
#include <catalog/pg_type.h>


extern bool metadata_is_hypertable(const char *schema_name, const char *table_name);
extern int metadata_insert_hypertable(const char *schema_name, const char *table_name);
extern int metadata_get_hypertable_id(const char *schema_name, const char *table_name);
extern void metadata_drop_hypertable(const char *schema_name, const char *table_name);

extern void metadata_insert_dimension(int hypertable_id,
                                    const char *column_name,
                                    Oid column_type,
                                    int64 interval_microseconds);

extern int64 metadata_get_chunk_interval(int hypertable_id);
extern int metadata_insert_chunk(int hypertable_id,
                                const char *schema_name,
                                const char *table_name,
                                int64 start_time,
                                int64 end_time);

extern int metadata_find_chunk(int hypertable_id, int64 time_microseconds);