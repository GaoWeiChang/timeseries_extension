#pragma once

#include <postgres.h>

extern void planner_hook_init(void);
extern void planner_hook_cleanup(void);