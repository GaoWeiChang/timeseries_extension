#!/bin/bash
set -e

EXTENSION_NAME="timeseries"

echo "========================================"
echo "  Timeseries Extension - Installer"
echo "========================================"


# verify root
if [ "$EUID" -ne 0 ]; then
    echo "ERROR: Please run as root: sudo ./install.sh"
    exit 1
fi


# Detect OS
detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS_ID="${ID}"          # eg. rhel
        OS_VER="${VERSION_ID}" # eg. 9.3
    else
        echo "ERROR: Cannot detect OS (no /etc/os-release)"
        exit 1
    fi
}
detect_os

echo ""
echo "  Detected OS : ${OS_ID} ${OS_VER}"


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


# Check files: simple_timeseries.so, simple_timeseries.control, simple_timeseries--1.0.sql
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MISSING=0
for f in simple_timeseries.so simple_timeseries.control "simple_timeseries--1.0.sql"; do
    if [ ! -f "${SCRIPT_DIR}/${f}" ]; then
        echo "ERROR: Missing file: ${f}"
        MISSING=1
    fi
done
[ $MISSING -eq 1 ] && exit 1


# Install files
echo ""
echo "[1/3] Installing simple_timeseries.so  -> ${PG_PKGLIBDIR}/"
install -m 755 "${SCRIPT_DIR}/simple_timeseries.so" "${PG_PKGLIBDIR}/simple_timeseries.so"

echo "[2/3] Installing simple_timeseries.control -> ${PG_EXTDIR}/"
install -m 644 "${SCRIPT_DIR}/simple_timeseries.control" "${PG_EXTDIR}/simple_timeseries.control"

echo "[3/3] Installing simple_timeseries--1.0.sql -> ${PG_EXTDIR}/"
install -m 644 "${SCRIPT_DIR}/simple_timeseries--1.0.sql" "${PG_EXTDIR}/simple_timeseries--1.0.sql"


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
    if systemctl is-active --quiet "${PG_SERVICE}"; then
        systemctl restart "${PG_SERVICE}"
        echo "PostgreSQL (${PG_SERVICE}) restarted."
    else
        echo "WARNING: ${PG_SERVICE} is not running. Start it manually."
    fi
else
    echo "WARNING: Could not find PostgreSQL service. Restart manually."
fi

echo ""
echo "=============================================="
echo "  Installation Complete!"
echo "=============================================="