#!/bin/bash

echo "Testing memory leak"

valgrind \
  --leak-check=full \
  --show-leak-kinds=all \
  --suppressions=postgres.supp \
  /usr/lib/postgresql/16/bin/postgres \
    --single -D /var/lib/postgresql/16/main postgres < test/test_utils.sql

