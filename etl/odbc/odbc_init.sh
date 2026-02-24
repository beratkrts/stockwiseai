#!/bin/sh
set -eu

DRIVER_NAME="FirebirdODBC"
DRIVER_PATH="/usr/lib/libOdbcFb.so"
DSN_NAME="${FB_ODBC_DSN:-test}"
FB_HOST="${FB_HOST:-127.0.0.1}"
FB_PORT="${FB_PORT:-3050}"
FB_DB_RAW="${FB_DB:-}"
FB_DB=$(printf "%s" "$FB_DB_RAW" | tr '\\' '/')
FB_USER="${FB_USER:-OWNER}"
FB_PASSWORD="${FB_PASSWORD:-}"
FB_CHARSET="${FB_CHARSET:-WIN1254}"

export ODBCINI="/etc/odbc.ini"
export ODBCSYSINI="/etc"
export ODBCINSTINI="odbcinst.ini"

cat >/etc/odbcinst.ini <<EOF
[ODBC Drivers]
$DRIVER_NAME=Installed

[$DRIVER_NAME]
Description=Firebird ODBC Driver
Driver=$DRIVER_PATH
EOF

cat >/etc/odbc.ini <<EOF
[ODBC Data Sources]
$DSN_NAME=$DRIVER_PATH

[$DSN_NAME]
Driver=$DRIVER_PATH
Driver64=$DRIVER_PATH
Driver32=$DRIVER_PATH
Setup=$DRIVER_PATH
Setup64=$DRIVER_PATH
Setup32=$DRIVER_PATH
DBNAME=$FB_HOST/$FB_PORT:$FB_DB
USER=$FB_USER
PASSWORD=$FB_PASSWORD
CHARSET=$FB_CHARSET
EOF

exec "$@"
