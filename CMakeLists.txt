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

# ===============================================
# กำหนด Source Files
# ===============================================

set(SOURCES
    src/init.c
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