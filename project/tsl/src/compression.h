#pragma once

#include <postgres.h>

#define NAMEDATALEN 64

// compress chunk (column oriented)
extern void compress_chunk_internal(int chunk_id);
