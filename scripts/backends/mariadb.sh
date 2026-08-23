#!/usr/bin/env bash
# MariaDB coordinator backend driver. Sourced by blackbox_lifecycle.sh.

blackbox_backend_initialize() {
  BLACKBOX_COMPOSE_PROFILE=mariadb
  BLACKBOX_DATABASE_SERVICE=mariadb
  BLACKBOX_DATABASE_CONTAINER=cdc-mariadb
  BLACKBOX_CONNECTOR_SERVICE=debezium
  BLACKBOX_CONNECTOR_CONTAINER=cdc-debezium
  BLACKBOX_DB_PORT=${BLACKBOX_DB_PORT:-3306}
  BLACKBOX_DB_USER=${BLACKBOX_DB_USER:-app}
  BLACKBOX_DB_PASSWORD=${BLACKBOX_DB_PASSWORD:-app_pw}
  BLACKBOX_BACKEND_SOURCE_ID=${BLACKBOX_BACKEND_SOURCE_ID:-mariadb-main}
  BLACKBOX_BENCHBASE_DATABASE_PROFILE=mariadb
  BLACKBOX_JDBC_URL=${BLACKBOX_JDBC_URL:-jdbc:mariadb://${BLACKBOX_DB_HOST}:${BLACKBOX_DB_PORT}/benchbase?sslMode=disable\&useServerPrepStmts=true}
  BLACKBOX_MARIADB_CLI=${BLACKBOX_MARIADB_CLI:-mariadb}
  BLACKBOX_ADAPTER_SCHEMA_FILE=
  export BLACKBOX_COMPOSE_PROFILE BLACKBOX_DATABASE_SERVICE
  export BLACKBOX_DATABASE_CONTAINER BLACKBOX_CONNECTOR_SERVICE
  export BLACKBOX_CONNECTOR_CONTAINER BLACKBOX_DB_PORT
  export BLACKBOX_DB_USER BLACKBOX_DB_PASSWORD BLACKBOX_MARIADB_CLI
  export BLACKBOX_BACKEND_SOURCE_ID
  export BLACKBOX_BENCHBASE_DATABASE_PROFILE BLACKBOX_JDBC_URL
}

blackbox_backend_prepare_run() {
  BLACKBOX_ADAPTER_SCHEMA_FILE="$BLACKBOX_ROOT/mariadb/initdb/adapters/${BLACKBOX_BENCHMARK}.sql"
  [[ -f $BLACKBOX_ADAPTER_SCHEMA_FILE ]] ||
    blackbox_die "selected MariaDB adapter schema is missing: $BLACKBOX_ADAPTER_SCHEMA_FILE"
  BLACKBOX_SET_NAMES_SQL=$(jq -er '.session_contract.configuration_statements[0]' \
    <<<"$BLACKBOX_BACKEND_CONTRACT") || return 1
  export BLACKBOX_ADAPTER_SCHEMA_FILE BLACKBOX_SET_NAMES_SQL
}

blackbox_backend_require_commands() {
  blackbox_require_command "$BLACKBOX_MARIADB_CLI"
}

blackbox_backend_teardown() {
  blackbox_compose down --volumes --remove-orphans
}

blackbox_backend_start_database_schema() {
  blackbox_compose up --detach --force-recreate redis "$BLACKBOX_DATABASE_SERVICE"
}

blackbox_backend_start_connector() {
  blackbox_compose up --detach --no-deps --force-recreate \
    "$BLACKBOX_CONNECTOR_SERVICE"
}

blackbox_backend_database() {
  # BLACKBOX_DB_HOST is initialized by the generic lifecycle before this
  # dynamically sourced driver executes.
  # shellcheck disable=SC2153
  local set_names_sql=${BLACKBOX_SET_NAMES_SQL:-SET NAMES utf8mb4 COLLATE utf8mb4_bin}
  "$BLACKBOX_MARIADB_CLI" --skip-ssl \
    --host="$BLACKBOX_DB_HOST" --port="$BLACKBOX_DB_PORT" \
    --user="$BLACKBOX_DB_USER" --password="$BLACKBOX_DB_PASSWORD" \
    --database=benchbase --default-character-set=utf8mb4 \
    --init-command="$set_names_sql" --batch --raw \
    --skip-column-names "$@"
}

blackbox_backend_database_query() {
  blackbox_backend_database --execute "$1"
}

blackbox_backend_capture_schema_metadata() {
  blackbox_backend_database_query "
    SELECT 'SETTING', 'server.character_set', @@GLOBAL.character_set_server
    UNION ALL SELECT 'SETTING', 'server.collation', @@GLOBAL.collation_server
    UNION ALL SELECT 'SETTING', 'server.time_zone', @@GLOBAL.time_zone
    UNION ALL SELECT 'SETTING', 'server.sql_mode', @@GLOBAL.sql_mode
    UNION ALL SELECT 'SETTING', 'server.transaction_isolation', @@GLOBAL.transaction_isolation
    UNION ALL SELECT 'SETTING', 'server.snapshot_isolation', @@GLOBAL.innodb_snapshot_isolation
    UNION ALL SELECT 'SETTING', 'session.character_set_client', @@SESSION.character_set_client
    UNION ALL SELECT 'SETTING', 'session.character_set_connection', @@SESSION.character_set_connection
    UNION ALL SELECT 'SETTING', 'session.character_set_results', @@SESSION.character_set_results
    UNION ALL SELECT 'SETTING', 'session.collation_connection', @@SESSION.collation_connection
    UNION ALL SELECT 'SETTING', 'session.time_zone', @@SESSION.time_zone
    UNION ALL SELECT 'SETTING', 'session.sql_mode', @@SESSION.sql_mode
    UNION ALL SELECT 'SETTING', 'session.transaction_isolation', @@SESSION.transaction_isolation
    UNION ALL SELECT 'SETTING', 'session.snapshot_isolation', @@SESSION.innodb_snapshot_isolation"
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

blackbox_backend_commit_fence() {
  local run_id=$1 phase=$2 nonce=$3
  blackbox_backend_database_query \
    "START TRANSACTION; INSERT INTO checker_run_fence (run_id, phase, nonce) VALUES ('$run_id', '$phase', '$nonce'); COMMIT;" >/dev/null
}

blackbox_backend_active_external_connections() {
  blackbox_backend_database_query \
    "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB = 'benchbase' AND USER <> 'debezium' AND ID <> CONNECTION_ID()"
}

blackbox_backend_wait_for_connector_progress() {
  local logs
  logs=$("$BLACKBOX_DOCKER" logs "$BLACKBOX_CONNECTOR_CONTAINER" 2>&1 || true)
  grep -q 'Starting streaming' <<<"$logs" || return 1
  grep -q 'connector.class = io.debezium.connector.mariadb.MariaDbConnector' \
    <<<"$logs" || return 1
  grep -q '3.6.1.Final' <<<"$logs" || return 1
  # The binlog client does not keep a default database selected. Require the
  # terminal connector-produced schema snapshot record in Redis instead of
  # inferring readiness from PROCESSLIST metadata or a partial stream prefix.
  local startup_evidence
  startup_evidence=$(blackbox_redis --json XRANGE cdc - +) || return 1
  jq -e '
    any(.[];
      (.[1][1] | fromjson) as $event |
      $event.source.snapshot? == "last")
  ' <<<"$startup_evidence" >/dev/null
}

blackbox_backend_capture_attestation_inputs() {
  local tables_file=$1 facts_file=$2 metadata_file
  metadata_file=$(mktemp /tmp/blackbox-mariadb-schema.XXXXXX) || return 1
  blackbox_backend_capture_schema_metadata >"$metadata_file" || {
    rm -f -- "$metadata_file"
    return 1
  }
  python3 "$BLACKBOX_RECORD_TOOL" attestation-tables \
    --benchmark "$BLACKBOX_BENCHMARK" >"$tables_file" || return 1
  local image_digest roster_digest table_digest privilege_digest
  image_digest=$("$BLACKBOX_DOCKER" inspect --format '{{.Image}}' \
    "$BLACKBOX_DATABASE_CONTAINER") || return 1
  image_digest=${image_digest#sha256:}
  roster_digest=$(jq -cS \
    '[.application_tables[], "benchbase.checker_run_fence", "benchbase.session_txn"] | sort' \
    <<<"$BLACKBOX_ADAPTER_CONTRACT" | sha256sum | cut -d' ' -f1) || return 1
  table_digest=$(sha256sum "$metadata_file" | cut -d' ' -f1) || return 1
  privilege_digest=$(blackbox_backend_database_query "SHOW GRANTS FOR 'app'@'%'" |
    LC_ALL=C sort | sha256sum | cut -d' ' -f1) || return 1
  jq -n \
    --arg image "$image_digest" --arg roster "$roster_digest" \
    --arg tables "$table_digest" --arg privileges "$privilege_digest" '
    [
      {name:"database_name",canonical_value:"benchbase"},
      {name:"schema_name",canonical_value:"benchbase"},
      {name:"database_character_set",canonical_value:"utf8mb4"},
      {name:"database_collation",canonical_value:"utf8mb4_bin"},
      {name:"application_table_engine",canonical_value:"InnoDB"},
      {name:"application_table_character_set",canonical_value:"utf8mb4"},
      {name:"application_table_collation",canonical_value:"utf8mb4_bin"},
      {name:"server_character_set",canonical_value:"utf8mb4"},
      {name:"server_collation",canonical_value:"utf8mb4_bin"},
      {name:"server_time_zone",canonical_value:"+00:00"},
      {name:"server_sql_mode",canonical_value:"STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH"},
      {name:"server_transaction_isolation",canonical_value:"REPEATABLE_READ"},
      {name:"server_snapshot_isolation",canonical_value:"true"},
      {name:"connector_class",canonical_value:"io.debezium.connector.mariadb.MariaDbConnector"},
      {name:"connector_source",canonical_value:"mariadb"},
      {name:"connector_version",canonical_value:"3.6.1.Final"},
      {name:"connector_tasks",canonical_value:"1"},
      {name:"redis_stream",canonical_value:"cdc"},
      {name:"database_image_digest",canonical_value:$image},
      {name:"capture_table_roster_sha256",canonical_value:$roster},
      {name:"table_definition_sha256",canonical_value:$tables},
      {name:"dedicated_privileges_sha256",canonical_value:$privileges}
    ]' >"$facts_file"
  rm -f -- "$metadata_file"
}

blackbox_backend_status() {
  blackbox_backend_database_query \
    "SELECT @@GLOBAL.gtid_binlog_pos, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image"
}
