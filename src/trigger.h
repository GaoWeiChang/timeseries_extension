#pragma once

#include <postgres.h>

extern void trigger_create_on_hypertable(const char *schema_name, const char *table_name);
extern void trigger_drop_on_hypertable(const char *schema_name, const char *table_name);
