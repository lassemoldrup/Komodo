#!/usr/bin/env bash
# Oracle Free/LogMiner coordinator backend driver. Sourced by the generic lifecycle.

blackbox_backend_initialize() {
  BLACKBOX_COMPOSE_PROFILE=oracle
  BLACKBOX_DATABASE_SERVICE=oracle
  BLACKBOX_DATABASE_CONTAINER=cdc-oracle
  BLACKBOX_CONNECTOR_SERVICE=debezium-oracle
  BLACKBOX_CONNECTOR_CONTAINER=cdc-debezium-oracle
  BLACKBOX_DB_PORT=${BLACKBOX_DB_PORT:-1521}
  BLACKBOX_DB_USER=blackbox_application
  BLACKBOX_BACKEND_SOURCE_ID=${BLACKBOX_BACKEND_SOURCE_ID:-oracle_blackbox_main}
  BLACKBOX_BENCHBASE_DATABASE_PROFILE=oracle
  BLACKBOX_BACKEND_MONITOR_ARGUMENTS=()
  export BLACKBOX_COMPOSE_PROFILE BLACKBOX_DATABASE_SERVICE
  export BLACKBOX_DATABASE_CONTAINER BLACKBOX_CONNECTOR_SERVICE
  export BLACKBOX_CONNECTOR_CONTAINER BLACKBOX_DB_PORT BLACKBOX_DB_USER
  export BLACKBOX_BACKEND_SOURCE_ID BLACKBOX_BENCHBASE_DATABASE_PROFILE
}

blackbox_backend_prepare_run() {
  BLACKBOX_ORACLE_SYS_PASSWORD=${BLACKBOX_ORACLE_SYS_PASSWORD:-BlackboxOracleSys_2026}
  BLACKBOX_ORACLE_OWNER_PASSWORD=${BLACKBOX_ORACLE_OWNER_PASSWORD:-BlackboxOwner_2026}
  BLACKBOX_ORACLE_APPLICATION_PASSWORD=${BLACKBOX_ORACLE_APPLICATION_PASSWORD:-BlackboxApplication_2026}
  BLACKBOX_ORACLE_LOGMINER_PASSWORD=${BLACKBOX_ORACLE_LOGMINER_PASSWORD:-BlackboxLogMiner_2026}
  BLACKBOX_DB_PASSWORD=$BLACKBOX_ORACLE_APPLICATION_PASSWORD
  BLACKBOX_JDBC_URL=${BLACKBOX_JDBC_URL:-jdbc:oracle:thin:@//${BLACKBOX_DB_HOST}:${BLACKBOX_DB_PORT}/FREEPDB1}
  BLACKBOX_ADAPTER_SCHEMA_FILE="$BLACKBOX_ROOT/oracle/initdb/adapters/${BLACKBOX_BENCHMARK}.sql"
  BLACKBOX_ADAPTER_WARMUP_FILE="$BLACKBOX_ROOT/oracle/initdb/adapters/${BLACKBOX_BENCHMARK}-warmup.sql"
  [[ -f $BLACKBOX_ADAPTER_SCHEMA_FILE ]] ||
    blackbox_die "selected Oracle adapter schema is missing: $BLACKBOX_ADAPTER_SCHEMA_FILE"
  [[ -f $BLACKBOX_ADAPTER_WARMUP_FILE ]] ||
    blackbox_die "selected Oracle adapter warmup is missing: $BLACKBOX_ADAPTER_WARMUP_FILE"
  local physical_tables
  physical_tables=$(jq -er '
    [.application_tables[], "benchbase.checker_run_fence", "benchbase.session_txn"] |
    map(split(".")[1] | ascii_upcase | "BLACKBOX_OWNER." + .) | join(",")
  ' <<<"$BLACKBOX_ADAPTER_CONTRACT") || return 1
  BLACKBOX_ORACLE_CAPTURE_TABLES=$physical_tables
  BLACKBOX_CAPTURE_PROFILE=${BLACKBOX_CAPTURE_PROFILE:-oracle_logminer_all_columns_v1}
  [[ $BLACKBOX_CAPTURE_PROFILE == oracle_logminer_all_columns_v1 ]] || {
    blackbox_die "unsupported Oracle physical capture profile: $BLACKBOX_CAPTURE_PROFILE"
    return 1
  }
  BLACKBOX_BACKEND_MONITOR_ARGUMENTS=(
    --expected-backend-fact "physical_capture_profile=$BLACKBOX_CAPTURE_PROFILE"
  )
  export BLACKBOX_ORACLE_SYS_PASSWORD BLACKBOX_ORACLE_OWNER_PASSWORD
  export BLACKBOX_ORACLE_APPLICATION_PASSWORD BLACKBOX_ORACLE_LOGMINER_PASSWORD
  export BLACKBOX_ORACLE_CAPTURE_TABLES BLACKBOX_DB_PASSWORD BLACKBOX_JDBC_URL
  export BLACKBOX_ADAPTER_SCHEMA_FILE BLACKBOX_ADAPTER_WARMUP_FILE
  export BLACKBOX_CAPTURE_PROFILE
}

blackbox_backend_require_commands() { :; }

blackbox_backend_teardown() {
  blackbox_compose_all_profiles down --volumes --remove-orphans
}

blackbox_backend_wait_for_schema_initialization() {
  local deadline=$(( $(date +%s) + ${BLACKBOX_STACK_TIMEOUT_SECONDS:-300} ))
  while (( $(date +%s) <= deadline )); do
    local state exit_code
    state=$($BLACKBOX_DOCKER inspect --format '{{.State.Status}}' cdc-oracle-init 2>/dev/null || true)
    exit_code=$($BLACKBOX_DOCKER inspect --format '{{.State.ExitCode}}' cdc-oracle-init 2>/dev/null || true)
    if [[ $state == exited && $exit_code == 0 ]]; then return 0; fi
    if [[ $state == exited && $exit_code != 0 ]]; then
      $BLACKBOX_DOCKER logs cdc-oracle-init >&2 || true
      blackbox_die "Oracle run-owned schema initialization failed"
      return 1
    fi
    sleep 1
  done
  blackbox_die "timed out waiting for Oracle schema initialization"
}

blackbox_backend_start_database_schema() {
  blackbox_compose up --detach --force-recreate redis oracle oracle_init
  blackbox_backend_wait_for_schema_initialization
}

blackbox_backend_start_uninstrumented_database() {
  blackbox_compose up --detach --force-recreate oracle oracle_init
  blackbox_backend_wait_for_schema_initialization
}

blackbox_backend_start_connector() {
  blackbox_compose up --detach --no-deps --force-recreate debezium-oracle
}

blackbox_backend_admin_query() {
  local sql=$1
  $BLACKBOX_DOCKER exec -i cdc-oracle sqlplus -s / as sysdba <<SQL
whenever sqlerror exit failure rollback
set heading off feedback off pagesize 0 linesize 32767 trimspool on tab off
$sql
exit
SQL
}

blackbox_backend_database() {
  local sql
  if [[ ${1:-} == --execute || ${1:-} == --command ]]; then
    sql=$2
  else
    sql=${1:-}
  fi
  [[ $sql == *';' ]] || sql+=';'
  # SQL*Plus requires coordinator statements on separate physical lines even
  # when each statement is semicolon-terminated.
  sql=${sql//;/$';\n'}
  $BLACKBOX_DOCKER exec -i cdc-oracle sqlplus -s \
    "blackbox_application/${BLACKBOX_DB_PASSWORD}@localhost:1521/FREEPDB1" <<SQL
whenever sqlerror exit failure rollback
set heading off feedback off pagesize 0 linesize 32767 trimspool on tab off
set colsep '	'
ALTER SESSION SET CURRENT_SCHEMA=BLACKBOX_OWNER;
ALTER SESSION SET TIME_ZONE='+00:00';
ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,';
ALTER SESSION SET NLS_DATE_LANGUAGE='AMERICAN';
ALTER SESSION SET NLS_COMP=BINARY;
ALTER SESSION SET NLS_SORT=BINARY;
$sql
exit
SQL
}

blackbox_backend_database_query() { blackbox_backend_database --execute "$1"; }

blackbox_backend_verify_session_isolation() {
  local result
  result=$(blackbox_backend_database_query \
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA')||'|'||SESSIONTIMEZONE FROM DUAL; ROLLBACK;") || return 1
  [[ ${result//[[:space:]]/} == BLACKBOX_OWNER\|+00:00 ]] || {
    blackbox_die "Oracle session/isolation contract differs"
    return 1
  }
}

blackbox_backend_capture_schema_metadata() {
  blackbox_backend_admin_query "
ALTER SESSION SET CONTAINER=FREEPDB1;
SELECT 'DATABASE'||CHR(9)||SYS_CONTEXT('USERENV','CON_NAME')||CHR(9)||
       (SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET')||CHR(9)||'BINARY'
  FROM DUAL;
SELECT 'TABLE'||CHR(9)||'benchbase.'||LOWER(t.table_name)||CHR(9)||
       CASE WHEN COUNT(g.log_group_name) > 0 THEN 'ALL_COLUMNS' ELSE 'NONE' END||CHR(9)||t.temporary
  FROM dba_tables t LEFT JOIN dba_log_groups g
    ON g.owner=t.owner AND g.table_name=t.table_name AND g.always='ALWAYS'
 WHERE t.owner='BLACKBOX_OWNER'
 GROUP BY t.table_name,t.temporary ORDER BY t.table_name;
SELECT 'COLUMN'||CHR(9)||'benchbase.'||LOWER(table_name)||CHR(9)||LOWER(column_name)||CHR(9)||
       column_id||CHR(9)||data_type||CHR(9)||nullable||CHR(9)||
       COALESCE(TO_CHAR(data_precision),'-')||CHR(9)||COALESCE(TO_CHAR(data_scale),'-')||CHR(9)||
       CASE WHEN data_type IN ('CHAR','VARCHAR2') THEN TO_CHAR(char_length) ELSE '-' END||CHR(9)||
       COALESCE(char_used,'-')||CHR(9)||identity_column
  FROM dba_tab_columns WHERE owner='BLACKBOX_OWNER' ORDER BY table_name,column_id;
SELECT 'PRIMARY'||CHR(9)||'benchbase.'||LOWER(c.table_name)||CHR(9)||LOWER(c.column_name)||CHR(9)||c.position
  FROM dba_constraints p JOIN dba_cons_columns c
    ON c.owner=p.owner AND c.constraint_name=p.constraint_name
 WHERE p.owner='BLACKBOX_OWNER' AND p.constraint_type='P'
 ORDER BY c.table_name,c.position;"
}

blackbox_backend_verify_schema() {
  blackbox_backend_capture_schema_metadata |
    python3 "$BLACKBOX_ROOT/scripts/backends/oracle_catalog.py" \
      --adapter "$BLACKBOX_ROOT/protocol/adapters/${BLACKBOX_BENCHMARK}.json"
}

blackbox_backend_count_table() {
  local table=$1
  [[ $table =~ ^benchbase\.([a-z][a-z0-9_]*)$ ]] || return 1
  blackbox_backend_database_query "SELECT COUNT(*) FROM \"${BASH_REMATCH[1]^^}\";"
}

blackbox_backend_marker_storage() {
  blackbox_backend_admin_query "
ALTER SESSION SET CONTAINER=FREEPDB1;
SELECT (SELECT COUNT(*) FROM blackbox_owner.session_txn)||CHR(9)||
       COALESCE((SELECT SUM(bytes) FROM dba_segments
                  WHERE owner='BLACKBOX_OWNER' AND
                        (segment_name='SESSION_TXN' OR segment_name IN
                          (SELECT index_name FROM dba_indexes WHERE owner='BLACKBOX_OWNER' AND table_name='SESSION_TXN'))),0)
  FROM DUAL;"
}

blackbox_backend_commit_fence() {
  local run_id=$1 phase=$2 nonce=$3
  blackbox_backend_database_query \
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; INSERT INTO checker_run_fence(run_id,phase,nonce) VALUES('$run_id','$phase','$nonce'); COMMIT;" >/dev/null
}

blackbox_backend_active_external_connections() {
  blackbox_backend_admin_query \
    "SELECT COUNT(*) FROM v\$session WHERE username='BLACKBOX_APPLICATION' AND status <> 'KILLED';"
}

blackbox_backend_wait_for_connector_progress() {
  local logs
  logs=$($BLACKBOX_DOCKER logs "$BLACKBOX_CONNECTOR_CONTAINER" 2>&1 || true)
  grep -q 'Starting streaming' <<<"$logs" || return 1
  grep -q 'connector.class = io.debezium.connector.oracle.OracleConnector' <<<"$logs" || return 1
  grep -q '3.6.1.Final' <<<"$logs" || return 1
  grep -q "table.include.list = $BLACKBOX_ORACLE_CAPTURE_TABLES" <<<"$logs" || return 1
  if grep -q "Connector completed: success = 'false'" <<<"$logs"; then return 2; fi
}

blackbox_backend_capture_attestation_inputs() {
  local tables_file=$1 facts_file=$2 metadata_file privilege_file
  metadata_file=$(mktemp /tmp/blackbox-oracle-schema.XXXXXX)
  privilege_file=$(mktemp /tmp/blackbox-oracle-privileges.XXXXXX)
  blackbox_backend_capture_schema_metadata >"$metadata_file" || return 1
  python3 "$BLACKBOX_RECORD_TOOL" attestation-tables \
    --benchmark "$BLACKBOX_BENCHMARK" >"$tables_file" || return 1
  blackbox_backend_admin_query "
ALTER SESSION SET CONTAINER=FREEPDB1;
SELECT grantee||CHR(9)||privilege FROM dba_sys_privs
 WHERE grantee IN ('BLACKBOX_APPLICATION','C##BLACKBOX_LOGMINER')
 UNION ALL SELECT grantee||CHR(9)||granted_role FROM dba_role_privs
 WHERE grantee IN ('BLACKBOX_APPLICATION','C##BLACKBOX_LOGMINER')
 UNION ALL SELECT grantee||CHR(9)||owner||'.'||table_name||':'||privilege FROM dba_tab_privs
 WHERE grantee IN ('BLACKBOX_APPLICATION','C##BLACKBOX_LOGMINER')
 ORDER BY 1;" >"$privilege_file" || return 1
  local image_digest roster_digest table_digest privilege_digest
  image_digest=$($BLACKBOX_DOCKER inspect --format '{{.Image}}' "$BLACKBOX_DATABASE_CONTAINER") || return 1
  image_digest=${image_digest#sha256:}
  roster_digest=$(jq -cS '[.application_tables[], "benchbase.checker_run_fence", "benchbase.session_txn"] | sort' \
    <<<"$BLACKBOX_ADAPTER_CONTRACT" | sha256sum | cut -d' ' -f1)
  table_digest=$(sha256sum "$metadata_file" | cut -d' ' -f1)
  privilege_digest=$(LC_ALL=C sort "$privilege_file" | sha256sum | cut -d' ' -f1)
  jq -n --arg image "$image_digest" --arg roster "$roster_digest" \
    --arg tables "$table_digest" --arg privileges "$privilege_digest" '
    [
      {name:"database_name",canonical_value:"FREEPDB1"},
      {name:"schema_name",canonical_value:"BLACKBOX_OWNER"},
      {name:"server_version",canonical_value:"Oracle AI Database 26ai Free 23.26.2.0.0"},
      {name:"database_character_set",canonical_value:"AL32UTF8"},
      {name:"nls_comp",canonical_value:"BINARY"},
      {name:"nls_sort",canonical_value:"BINARY"},
      {name:"session_time_zone",canonical_value:"+00:00"},
      {name:"session_transaction_isolation",canonical_value:"SERIALIZABLE"},
      {name:"bootstrap_loader_isolation",canonical_value:"READ_COMMITTED"},
      {name:"archive_log_mode",canonical_value:"ARCHIVELOG"},
      {name:"force_logging",canonical_value:"YES"},
      {name:"supplemental_log_data_min",canonical_value:"YES"},
      {name:"online_redo_log_groups",canonical_value:"6"},
      {name:"online_redo_large_groups",canonical_value:"4"},
      {name:"online_redo_large_group_bytes",canonical_value:"536870912"},
      {name:"table_supplemental_logging",canonical_value:"ALL_COLUMNS"},
      {name:"log_mining_strategy",canonical_value:"online_catalog"},
      {name:"log_mining_batch_size_default",canonical_value:"20000"},
      {name:"log_mining_sleep_profile",canonical_value:"0_200_1000_200"},
      {name:"physical_capture_profile",canonical_value:"oracle_logminer_all_columns_v1"},
      {name:"connector_class",canonical_value:"io.debezium.connector.oracle.OracleConnector"},
      {name:"connector_source",canonical_value:"oracle"},
      {name:"connector_version",canonical_value:"3.6.1.Final"},
      {name:"connector_sha256",canonical_value:"b3a81a6d4e0822788a2c1a04bfd6007adcfedd0d07edeb6deebd8df73834fed8"},
      {name:"connector_ojdbc_sha256",canonical_value:"ef295bf01374dac7d791b3bc6526d10e8910ecac4645a95a3ca0c01cfc5c21d0"},
      {name:"benchbase_ojdbc_sha256",canonical_value:"ec8b7f2020b03b19f572e1bc34f94330610e86d3113ffe1e1f0474b8f5ce88ed"},
      {name:"connector_tasks",canonical_value:"1"},
      {name:"redis_stream",canonical_value:"cdc"},
      {name:"database_image_digest",canonical_value:$image},
      {name:"capture_table_roster_sha256",canonical_value:$roster},
      {name:"table_definition_sha256",canonical_value:$tables},
      {name:"dedicated_privileges_sha256",canonical_value:$privileges}
    ]' >"$facts_file"
  rm -f -- "$metadata_file" "$privilege_file"
}

blackbox_backend_status() {
  blackbox_backend_admin_query \
    "SELECT current_scn||CHR(9)||log_mode||CHR(9)||force_logging||CHR(9)||supplemental_log_data_min FROM v\$database;"
}
