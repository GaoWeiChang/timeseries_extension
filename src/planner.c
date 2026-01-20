#include <postgres.h>
#include <optimizer/planner.h>
#include <optimizer/pathnode.h>
#include <nodes/pg_list.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/inval.h>
#include <executor/spi.h>
#include <nodes/makefuncs.h>
#include <parser/parsetree.h>
#include <access/xact.h>

#include "metadata.h"

static planner_hook_type prev_planner_hook = NULL;

typedef struct HypertableCacheKey
{
    Oid relid;
} HypertableCacheKey;

typedef struct HypertableCacheEntry
{
    Oid relid;
    bool is_hypertable;
    char schema_name[NAMEDATALEN];
    char table_name[NAMEDATALEN];
} HypertableCacheEntry;

static HTAB *hypertable_cache = NULL;
static bool cache_valid = false;

static void init_hypertable_cache(void);
static void invalidate_cache_callback(XactEvent event, void *arg);
static void rebuild_hypertable_cache(void);

