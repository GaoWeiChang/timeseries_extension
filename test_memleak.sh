#!/bin/bash

echo "Testing memory leak"

# Default memory leak value (from postgres)
# ==3868== LEAK SUMMARY:
# ==3868==    definitely lost: 152 bytes in 1 blocks
# ==3868==    indirectly lost: 619 bytes in 18 blocks
# ==3868==      possibly lost: 0 bytes in 0 blocks
# ==3868==    still reachable: 187,609 bytes in 25 blocks
# ==3868==         suppressed: 0 bytes in 0 blocks

valgrind \
  --leak-check=full \
  --show-leak-kinds=all \
  /usr/lib/postgresql/16/bin/postgres \
    --single -D /var/lib/postgresql/16/main postgres < test/test_metadata.sql

