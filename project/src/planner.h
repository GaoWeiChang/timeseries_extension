#pragma once

#include <postgres.h>

void planner_hook_init(void);
void planner_hook_cleanup(void);
void planner_invalidate_cache(void);