#!/usr/bin/env bash

blackbox_backend_initialize() {
  BLACKBOX_COMPOSE_PROFILE=oceanbase_mysql
  BLACKBOX_DATABASE_SERVICE=oceanbase-mysql
  BLACKBOX_DATABASE_CONTAINER=cdc-oceanbase-mysql
  BLACKBOX_CONNECTOR_SERVICE=debezium-oceanbase-mysql
  BLACKBOX_CONNECTOR_CONTAINER=cdc-debezium-oceanbase-mysql
  BLACKBOX_DB_PORT=${BLACKBOX_DB_PORT:-2883}
  BLACKBOX_DB_USER=${BLACKBOX_DB_USER:-blackbox_application@blackbox}
  BLACKBOX_DB_PASSWORD=${BLACKBOX_DB_PASSWORD:-OceanBaseApplication_2026!}
  BLACKBOX_BACKEND_SOURCE_ID=${BLACKBOX_BACKEND_SOURCE_ID:-oceanbase_mysql_main}
  BLACKBOX_BENCHBASE_DATABASE_PROFILE=oceanbase
  BLACKBOX_JDBC_URL=${BLACKBOX_JDBC_URL:-jdbc:oceanbase://${BLACKBOX_DB_HOST}:${BLACKBOX_DB_PORT}/benchbase?useSSL=false\&useUnicode=true\&characterEncoding=UTF-8}
  BLACKBOX_BACKEND_MONITOR_ARGUMENTS=()
  export BLACKBOX_COMPOSE_PROFILE BLACKBOX_DATABASE_SERVICE
  export BLACKBOX_DATABASE_CONTAINER BLACKBOX_CONNECTOR_SERVICE
  export BLACKBOX_CONNECTOR_CONTAINER BLACKBOX_DB_PORT
  export BLACKBOX_DB_USER BLACKBOX_DB_PASSWORD BLACKBOX_BACKEND_SOURCE_ID
  export BLACKBOX_BENCHBASE_DATABASE_PROFILE BLACKBOX_JDBC_URL
}

blackbox_backend_prepare_run() {
  BLACKBOX_ADAPTER_SCHEMA_FILE="$BLACKBOX_ROOT/oceanbase_mysql/initdb/adapters/${BLACKBOX_BENCHMARK}.sql"
  [[ -f $BLACKBOX_ADAPTER_SCHEMA_FILE ]] || {
    blackbox_die "selected OceanBase adapter schema is missing: $BLACKBOX_ADAPTER_SCHEMA_FILE"
    return 1
  }
  BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES=$(jq -er '
    [.application_tables[], "benchbase.checker_run_fence", "benchbase.session_txn"] | join(",")
  ' <<<"$BLACKBOX_ADAPTER_CONTRACT") || return 1
  BLACKBOX_CAPTURE_PROFILE=${BLACKBOX_CAPTURE_PROFILE:-oceanbase_obbinlog_full_row_v1}
  [[ $BLACKBOX_CAPTURE_PROFILE == oceanbase_obbinlog_full_row_v1 ]] || {
    blackbox_die "unsupported OceanBase capture profile: $BLACKBOX_CAPTURE_PROFILE"
    return 1
  }
  # shellcheck disable=SC2034
  BLACKBOX_BACKEND_MONITOR_ARGUMENTS=(
    --expected-backend-fact "physical_capture_profile=$BLACKBOX_CAPTURE_PROFILE"
  )
  export BLACKBOX_ADAPTER_SCHEMA_FILE BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES
  export BLACKBOX_CAPTURE_PROFILE
}

blackbox_backend_require_commands() { :; }

blackbox_backend_teardown() {
  blackbox_compose_all_profiles down --volumes --remove-orphans
}

blackbox_backend_start_database_schema() {
  blackbox_compose up --detach --force-recreate redis "$BLACKBOX_DATABASE_SERVICE"
}

blackbox_backend_start_uninstrumented_database() {
  blackbox_compose up --detach --force-recreate "$BLACKBOX_DATABASE_SERVICE"
}

blackbox_backend_start_connector() {
  blackbox_compose up --detach --no-deps --force-recreate "$BLACKBOX_CONNECTOR_SERVICE"
}

blackbox_backend_database() {
  local sql
  if [[ ${1:-} == --execute || ${1:-} == --command ]]; then
    sql=$2
  else
    sql=${1:-}
  fi
  # Stream large bootstrap results. This pinned obclient can silently return no
  # rows for a large buffered result even though the query exits successfully.
  "$BLACKBOX_DOCKER" exec "$BLACKBOX_DATABASE_CONTAINER" obclient --quick -N -s -A \
    -h127.0.0.1 -P2883 -u"$BLACKBOX_DB_USER" -p"$BLACKBOX_DB_PASSWORD" \
    -Dbenchbase \
    -e "SET NAMES utf8mb4 COLLATE utf8mb4_bin; SET time_zone='+00:00'; SET SESSION ob_read_consistency='STRONG'; SET SESSION sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'; $sql"
}

blackbox_backend_database_query() { blackbox_backend_database --execute "$1"; }

blackbox_backend_admin_query() {
  "$BLACKBOX_DOCKER" exec "$BLACKBOX_DATABASE_CONTAINER" obclient -N -s -A \
    -h127.0.0.1 -P2881 -uroot@blackbox \
    -p"${BLACKBOX_OCEANBASE_MYSQL_ROOT_PASSWORD:-OceanBaseContract_2026}" \
    -e "$1"
}

blackbox_backend_verify_session_isolation() {
  local result
  result=$(blackbox_backend_database_query \
    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT @@transaction_isolation,@@ob_read_consistency,@@collation_connection,@@time_zone,@@sql_mode;") || return 1
  [[ $result == $'REPEATABLE-READ\tSTRONG\tutf8mb4_bin\t+00:00\tSTRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' ]] || {
    blackbox_die "OceanBase session/isolation contract differs"
    return 1
  }
}

blackbox_backend_capture_schema_metadata() {
  blackbox_backend_database_query "
    SELECT 'SETTING', 'server.character_set', @@GLOBAL.character_set_server
    UNION ALL SELECT 'SETTING', 'server.collation', @@GLOBAL.collation_server
    UNION ALL SELECT 'SETTING', 'server.time_zone', @@GLOBAL.time_zone
    UNION ALL SELECT 'SETTING', 'server.sql_mode', @@GLOBAL.sql_mode
    UNION ALL SELECT 'SETTING', 'server.transaction_isolation', @@GLOBAL.transaction_isolation
    UNION ALL SELECT 'SETTING', 'server.snapshot_isolation', '1'
    UNION ALL SELECT 'SETTING', 'session.character_set_client', @@SESSION.character_set_client
    UNION ALL SELECT 'SETTING', 'session.character_set_connection', @@SESSION.character_set_connection
    UNION ALL SELECT 'SETTING', 'session.character_set_results', @@SESSION.character_set_results
    UNION ALL SELECT 'SETTING', 'session.collation_connection', @@SESSION.collation_connection
    UNION ALL SELECT 'SETTING', 'session.time_zone', @@SESSION.time_zone
    UNION ALL SELECT 'SETTING', 'session.sql_mode', @@SESSION.sql_mode
    UNION ALL SELECT 'SETTING', 'session.transaction_isolation', 'REPEATABLE_READ'
    UNION ALL SELECT 'SETTING', 'session.snapshot_isolation', '1'"
  blackbox_backend_database_query "
    SELECT 'DATABASE', SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
      FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'benchbase'"
  blackbox_backend_database_query "
    SELECT 'TABLE', CONCAT(TABLE_SCHEMA, '.', TABLE_NAME), ENGINE, TABLE_COLLATION
      FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'benchbase' ORDER BY TABLE_NAME"
  blackbox_backend_database_query "
    SELECT 'COLUMN', CONCAT(TABLE_SCHEMA, '.', TABLE_NAME), COLUMN_NAME,
           ORDINAL_POSITION, LOWER(DATA_TYPE), IS_NULLABLE,
           COALESCE(CAST(NUMERIC_PRECISION AS CHAR), '-'),
           COALESCE(CAST(NUMERIC_SCALE AS CHAR), '-'),
           COALESCE(CAST(CHARACTER_MAXIMUM_LENGTH AS CHAR), '-'),
           COALESCE(CAST(DATETIME_PRECISION AS CHAR), '-'),
           COALESCE(CHARACTER_SET_NAME, '-'), COALESCE(COLLATION_NAME, '-'),
           COALESCE(NULLIF(EXTRA, ''), '-')
      FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'benchbase'
     ORDER BY TABLE_NAME, ORDINAL_POSITION"
  blackbox_backend_database_query "
    SELECT 'PRIMARY', CONCAT(TABLE_SCHEMA, '.', TABLE_NAME), COLUMN_NAME,
           ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE
     WHERE TABLE_SCHEMA = 'benchbase' AND CONSTRAINT_NAME = 'PRIMARY'
     ORDER BY TABLE_NAME, ORDINAL_POSITION"
}

blackbox_backend_verify_schema() {
  blackbox_backend_capture_schema_metadata |
    python3 "$BLACKBOX_RECORD_TOOL" schema-check \
      --backend "$BLACKBOX_BACKEND" --benchmark "$BLACKBOX_BENCHMARK"
}

blackbox_backend_count_table() {
  local table=$1
  [[ $table =~ ^benchbase\.([a-z][a-z0-9_]*)$ ]] || return 1
  blackbox_backend_database_query "SELECT COUNT(*) FROM \`${BASH_REMATCH[1]}\`"
}

blackbox_backend_marker_storage() {
  blackbox_backend_database_query "
    SELECT (SELECT COUNT(*) FROM session_txn),
           COALESCE((SELECT DATA_LENGTH + INDEX_LENGTH FROM information_schema.TABLES
                      WHERE TABLE_SCHEMA='benchbase' AND TABLE_NAME='session_txn'),0)"
}

blackbox_backend_commit_fence() {
  local run_id=$1 phase=$2 nonce=$3
  blackbox_backend_database_query \
    "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION; INSERT INTO checker_run_fence(run_id,phase,nonce) VALUES('$run_id','$phase','$nonce'); COMMIT;" >/dev/null
}

blackbox_backend_active_external_connections() {
  blackbox_backend_admin_query \
    "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB='benchbase' AND USER NOT IN ('blackbox_connector','root')"
}

blackbox_backend_wait_for_connector_progress() {
  local logs metrics
  logs=$("$BLACKBOX_DOCKER" logs "$BLACKBOX_CONNECTOR_CONTAINER" 2>&1 || true)
  grep -q 'Starting streaming' <<<"$logs" || return 1
  grep -q 'connector.class = io.debezium.connector.mysql.MySqlConnector' <<<"$logs" || return 1
  grep -q '3.6.1.Final' <<<"$logs" || return 1
  grep -q "table.include.list = $BLACKBOX_OCEANBASE_MYSQL_CAPTURE_TABLES" <<<"$logs" || return 1
  if grep -q "Connector completed: success = 'false'" <<<"$logs"; then return 2; fi
  metrics=$("$BLACKBOX_DOCKER" exec "$BLACKBOX_DATABASE_CONTAINER" \
    curl -fsS http://127.0.0.1:2984/metrics) || return 1
  grep -q '^binlog_instance_num{.*} 1$' <<<"$metrics" || return 1
  grep -q '^binlog_instance_dump_count{.*} 1$' <<<"$metrics" || return 1
}

blackbox_backend_capture_attestation_inputs() {
  local tables_file=$1 facts_file=$2 metadata_file privilege_file metrics
  metadata_file=$(mktemp /tmp/blackbox-oceanbase-mysql-schema.XXXXXX)
  privilege_file=$(mktemp /tmp/blackbox-oceanbase-mysql-privileges.XXXXXX)
  blackbox_backend_capture_schema_metadata >"$metadata_file" || return 1
  python3 "$BLACKBOX_RECORD_TOOL" attestation-tables \
    --benchmark "$BLACKBOX_BENCHMARK" >"$tables_file" || return 1
  {
    blackbox_backend_admin_query "SHOW GRANTS FOR blackbox_application"
    blackbox_backend_admin_query "SHOW GRANTS FOR blackbox_connector"
  } | LC_ALL=C sort >"$privilege_file" || return 1
  metrics=$("$BLACKBOX_DOCKER" exec "$BLACKBOX_DATABASE_CONTAINER" \
    curl -fsS http://127.0.0.1:2984/metrics) || return 1
  grep -q '^binlog_instance_num{.*} 1$' <<<"$metrics" || return 1
  grep -q '^binlog_instance_dump_count{.*} 1$' <<<"$metrics" || return 1
  local image_digest roster_digest table_digest privilege_digest
  image_digest=$("$BLACKBOX_DOCKER" inspect --format '{{.Image}}' \
    "$BLACKBOX_DATABASE_CONTAINER") || return 1
  image_digest=${image_digest#sha256:}
  roster_digest=$(jq -cS \
    '[.application_tables[], "benchbase.checker_run_fence", "benchbase.session_txn"] | sort' \
    <<<"$BLACKBOX_ADAPTER_CONTRACT" | sha256sum | cut -d' ' -f1)
  table_digest=$(sha256sum "$metadata_file" | cut -d' ' -f1)
  privilege_digest=$(sha256sum "$privilege_file" | cut -d' ' -f1)
  jq -n --arg image "$image_digest" --arg roster "$roster_digest" \
    --arg tables "$table_digest" --arg privileges "$privilege_digest" '
    [
      {name:"database_name",canonical_value:"benchbase"},
      {name:"schema_name",canonical_value:"benchbase"},
      {name:"server_version",canonical_value:"4.4.2.1"},
      {name:"server_revision",canonical_value:"101000022026050611-8cf64ed50606966fd5c29f47265cf557d97ea776"},
      {name:"mysql_compatibility_version",canonical_value:"5.7.25-OceanBase_CE-v4.4.2.1"},
      {name:"database_character_set",canonical_value:"utf8mb4"},
      {name:"database_collation",canonical_value:"utf8mb4_bin"},
      {name:"application_table_engine",canonical_value:"InnoDB"},
      {name:"application_table_character_set",canonical_value:"utf8mb4"},
      {name:"application_table_collation",canonical_value:"utf8mb4_bin"},
      {name:"server_character_set",canonical_value:"utf8mb4"},
      {name:"server_collation",canonical_value:"utf8mb4_bin"},
      {name:"server_time_zone",canonical_value:"+00:00"},
      {name:"server_sql_mode",canonical_value:"STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"},
      {name:"server_transaction_isolation",canonical_value:"REPEATABLE_READ"},
      {name:"server_snapshot_isolation",canonical_value:"true"},
      {name:"session_transaction_isolation",canonical_value:"REPEATABLE_READ"},
      {name:"session_read_consistency",canonical_value:"STRONG"},
      {name:"session_time_zone",canonical_value:"+00:00"},
      {name:"binlog_row_image",canonical_value:"FULL"},
      {name:"binlog_format",canonical_value:"ROW"},
      {name:"gtid_mode",canonical_value:"ON"},
      {name:"binlog_checksum",canonical_value:"CRC32"},
      {name:"odp_version",canonical_value:"4.3.6.1"},
      {name:"obbinlog_version",canonical_value:"4.3.6.4"},
      {name:"binlog_instances",canonical_value:"1"},
      {name:"binlog_subscribers",canonical_value:"1"},
      {name:"failover_enabled",canonical_value:"false"},
      {name:"physical_capture_profile",canonical_value:"oceanbase_obbinlog_full_row_v1"},
      {name:"connector_class",canonical_value:"io.debezium.connector.mysql.MySqlConnector"},
      {name:"connector_source",canonical_value:"mysql"},
      {name:"connector_version",canonical_value:"3.6.1.Final"},
      {name:"connector_sha256",canonical_value:"db56640086983fa838aaedffd7a2fc9887932fc028dbbfef11f62fc49248d5a2"},
      {name:"connector_jdbc_sha256",canonical_value:"66df1d453789dc8cb759a7dc17f58646893bf28483f262328650f170472a6ead"},
      {name:"benchbase_jdbc_sha256",canonical_value:"783b1f07de6205548d8caae62dea4d51156fffe6056f8bae172b3475cccf6864"},
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
  "$BLACKBOX_DOCKER" exec "$BLACKBOX_DATABASE_CONTAINER" bash -lc '
    obclient -A -c -h127.0.0.1 -P2883 -ublackbox_connector@blackbox \
      -pOceanBaseConnector_2026! -e "SHOW MASTER STATUS;";
    curl -fsS http://127.0.0.1:2984/metrics |
      grep -E "^(binlog_instance_(num|dump_count|convert_checkpoint|convert_delay))"
  '
}
