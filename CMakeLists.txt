cmake_minimum_required(VERSION 3.10)
project(simple_timeseries)

# ==========================================
# Find postgesql file path
# ==========================================

# server header files (postgres.h, fmgr.h, etc.)
execute_process(
    COMMAND pg_config --includedir-server
    OUTPUT_VARIABLE PG_INCLUDEDIR_SERVER
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# client header files
execute_process(
    COMMAND pg_config --includedir
    OUTPUT_VARIABLE PG_INCLUDEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# .so file
execute_process(
    COMMAND pg_config --pkglibdir
    OUTPUT_VARIABLE PG_PKGLIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# .sql and .control file
execute_process(
    COMMAND pg_config --sharedir
    OUTPUT_VARIABLE PG_SHAREDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# ==========================================
# Setting up compiler
# ==========================================

# warning flag
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")

# debug
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -fno-omit-frame-pointer")


# ===============================================
# กำหนด Source Files
# ===============================================

set(SOURCES
    src/init.c
    src/utils.c
    src/metadata.c
    src/hypertable.c
    src/chunk.c
    src/trigger.c
    src/planner.c
)

# ===============================================
# create shared Librarl (.so)
# ===============================================

add_library(simple_timeseries MODULE ${SOURCES})
target_include_directories(simple_timeseries PRIVATE 
    ${PG_INCLUDEDIR_SERVER}
    ${PG_INCLUDEDIR}
)

set_target_properties(simple_timeseries PROPERTIES PREFIX "") # remove prefix (eg. libsimple_timeseries.so -> simple_timeseries.so)

# ===============================================
# Install Targets
# ===============================================

# install .so file
install(TARGETS simple_timeseries
        LIBRARY DESTINATION ${PG_PKGLIBDIR})

# install .control file
install(FILES simple_timeseries.control
        DESTINATION ${PG_SHAREDIR}/extension)

# install .sql files
install(FILES sql/simple_timeseries--1.0.sql
        DESTINATION ${PG_SHAREDIR}/extension)