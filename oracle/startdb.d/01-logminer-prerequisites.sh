#!/usr/bin/env bash
# Database-lifetime Oracle prerequisites only. Application schema and capture
# membership are created later by the run-owned oracle_init service.
set -euo pipefail

rm -f /tmp/blackbox-oracle-prerequisites.done

case ${BLACKBOX_ORACLE_LOGMINER_PASSWORD:-} in
  ''|*[!A-Za-z0-9_]* )
    echo "BLACKBOX_ORACLE_LOGMINER_PASSWORD must contain only letters, digits, and underscore" >&2
    exit 1
    ;;
esac

mode=$(sqlplus -s / as sysdba <<'SQL' | tr -d ' \n\r'
set heading off feedback off pagesize 0
select log_mode from v$database;
exit
SQL
)

if [[ $mode != ARCHIVELOG ]]; then
  sqlplus -s / as sysdba <<'SQL'
whenever sqlerror exit failure rollback
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
alter pluggable database all open;
exit
SQL
fi

# Allow crash-recovery/undo cleanup from the local ARCHIVELOG restart to
# quiesce before enabling supplemental logging. Issuing the DDL immediately
# can wait on Oracle's internal transaction-abort cleanup.
sleep 10

set +e
timeout --signal=TERM --kill-after=10 60 sqlplus -s / as sysdba <<'SQL'
whenever sqlerror continue
ALTER DATABASE FORCE LOGGING;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
exit
SQL
logging_status=$?
set -e

logging_facts=$(sqlplus -s / as sysdba <<'SQL' | tr -d ' \r'
set heading off feedback off pagesize 0
select log_mode||'|'||force_logging||'|'||supplemental_log_data_min from v$database;
exit
SQL
)
if [[ $logging_facts != 'ARCHIVELOG|YES|YES' ]]; then
  echo "Oracle logging prerequisites failed (sqlplus status $logging_status): $logging_facts" >&2
  exit 1
fi
sleep 2

# Oracle Free ships with only two 10 MiB online redo groups. A normal TPC-C
# load can recycle one while LogMiner still has it open, which fails closed as
# ORA-01368. Add four pinned large groups before any run-owned schema exists;
# the original groups may remain, but a fresh database switches into the large
# ring before the loader can wrap it.
sqlplus -s / as sysdba <<'SQL'
whenever sqlerror exit failure rollback
DECLARE
  present NUMBER;
  PROCEDURE add_group(group_number NUMBER, member_path VARCHAR2) IS
  BEGIN
    SELECT COUNT(*) INTO present FROM v$log WHERE group# = group_number;
    IF present = 0 THEN
      EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE GROUP '||group_number||
        ' ('''||member_path||''') SIZE 512M';
    END IF;
  END;
BEGIN
  add_group(3, '/opt/oracle/oradata/FREE/redo03.log');
  add_group(4, '/opt/oracle/oradata/FREE/redo04.log');
  add_group(5, '/opt/oracle/oradata/FREE/redo05.log');
  add_group(6, '/opt/oracle/oradata/FREE/redo06.log');
END;
/
exit
SQL

redo_facts=$(sqlplus -s / as sysdba <<'SQL' | tr -d ' \n\r'
set heading off feedback off pagesize 0
select count(*)||'|'||sum(case when bytes=536870912 then 1 else 0 end)
  from v$log;
exit
SQL
)
if [[ $redo_facts != '6|4' ]]; then
  echo "Oracle online redo prerequisites differ: $redo_facts" >&2
  exit 1
fi

sqlplus -s / as sysdba <<SQL
whenever sqlerror exit failure rollback
define logminer_password=${BLACKBOX_ORACLE_LOGMINER_PASSWORD}
@/container-entrypoint-startdb.d/logminer-prerequisites.sql.inc
exit
SQL

touch /tmp/blackbox-oracle-prerequisites.done
