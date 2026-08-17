#!/usr/bin/env bash
# Prepare Oracle for Debezium LogMiner CDC. Runs on every container start via
# gvenzl's /container-entrypoint-startdb.d hook, and is idempotent.
#
# This has to run INSIDE the container rather than from an init container,
# because enabling ARCHIVELOG requires MOUNT state, i.e. a local
# shutdown/startup through a bequeath (`sqlplus / as sysdba`) connection.
# A remote SYSDBA session cannot do it: the listener disappears with the
# instance, so it can never issue the STARTUP.
#
# Note ORACLE_ENABLE_ARCHIVELOG has no effect on these images: they ship a
# prebuilt database, so the setup path that honours that variable is skipped.
set -uo pipefail
rm -f /tmp/cdc-setup.done

log() { echo "[cdc-setup] $*"; }

MODE=$(sqlplus -s / as sysdba <<'EOF' 2>/dev/null | tr -d ' \n\r'
set heading off feedback off pagesize 0
select log_mode from v$database;
exit
EOF
)

if [ "$MODE" = "ARCHIVELOG" ]; then
  log "already in ARCHIVELOG mode, skipping restart"
else
  log "enabling ARCHIVELOG (requires a local restart) ..."
  sqlplus -s / as sysdba <<'EOF' >/dev/null 2>&1
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
alter pluggable database all open;
exit
EOF
  log "ARCHIVELOG enabled"
fi

log "applying force logging, supplemental logging, CDC user and schema ..."
sqlplus -s / as sysdba @/container-entrypoint-startdb.d/02-cdc-setup.sql >/tmp/cdc-setup.log 2>&1
# Expected on re-run: user exists (01920), object exists (00955), PK dup
# (00001 / 03301), force logging already on (12920), supplemental log group
# already exists (32588).
grep -iE 'ORA-[0-9]+' /tmp/cdc-setup.log \
  | grep -viE 'ORA-01920|ORA-00955|ORA-00001|ORA-03301|ORA-12920|ORA-32588' \
  && log "WARNING: unexpected errors above (see /tmp/cdc-setup.log)"

# Sentinel for the container healthcheck: cheaper and far more reliable than
# spawning sqlplus, which can exceed a short healthcheck timeout.
touch /tmp/cdc-setup.done
log "done."
