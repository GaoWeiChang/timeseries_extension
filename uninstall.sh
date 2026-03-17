#!/bin/bash
set -e

echo "========================================"
echo "  Timeseries Extension - Uninstaller"
echo "========================================"

if [ "$EUID" -ne 0 ]; then
    echo "ERROR: Please run as root: sudo ./uninstall.sh"
    exit 1
fi


# Detect pg_config 
PG_CONFIG=$(ls /usr/pgsql-*/bin/pg_config 2>/dev/null | sort -V | tail -1)

# check PATH is not empty
if [ -n "$PG_CONFIG" ]; then
    export PATH="$(dirname "$PG_CONFIG"):$PATH"
else
    echo "ERROR: pg_config not found."
    exit 1
fi

PG_PKGLIBDIR=$(pg_config --pkglibdir)
PG_SHAREDIR=$(pg_config --sharedir)
PG_EXTDIR="${PG_SHAREDIR}/extension"

echo "Removing extension files..."
rm -f "${PG_PKGLIBDIR}/simple_timeseries.so"
rm -f "${PG_EXTDIR}/simple_timeseries.control"
rm -f "${PG_EXTDIR}/simple_timeseries--1.0.sql"
echo "Files removed."


# Restart PostgreSQL
echo ""
echo "Restarting PostgreSQL..."

# find service name
PG_SERVICE=""
for svc in postgresql-16 postgresql-17; do
    if systemctl list-units --type=service 2>/dev/null | grep -q "${svc}.service"; then
        PG_SERVICE="${svc}"
        break
    fi
done

if [ -n "$PG_SERVICE" ]; then
    echo "Restarting PostgreSQL (${PG_SERVICE})..."
    systemctl restart "${PG_SERVICE}"
fi

echo ""
echo "Extension removed."