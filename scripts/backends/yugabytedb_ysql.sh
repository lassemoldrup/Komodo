#!/usr/bin/env bash
# YugabyteDB YSQL coordinator backend driver. Sourced by blackbox_lifecycle.sh.

blackbox_backend_initialize() {
  BLACKBOX_COMPOSE_PROFILE=yugabytedb_ysql
  BLACKBOX_DATABASE_SERVICE=yugabytedb_ysql
  BLACKBOX_DATABASE_CONTAINER=cdc-yugabytedb-ysql
  BLACKBOX_CONNECTOR_SERVICE=debezium_yugabytedb_ysql
  BLACKBOX_CONNECTOR_CONTAINER=cdc-debezium-yugabytedb-ysql
  BLACKBOX_DB_PORT=${BLACKBOX_DB_PORT:-5433}
  BLACKBOX_DB_USER=${BLACKBOX_DB_USER:-blackbox_application}
  BLACKBOX_DB_PASSWORD=${BLACKBOX_DB_PASSWORD:-blackbox_application_password}
  BLACKBOX_BACKEND_SOURCE_ID=${BLACKBOX_BACKEND_SOURCE_ID:-yugabytedb_ysql_main}
  BLACKBOX_BACKEND_MONITOR_ARGUMENTS=()
  BLACKBOX_BENCHBASE_DATABASE_PROFILE=postgres
  BLACKBOX_JDBC_URL=${BLACKBOX_JDBC_URL:-jdbc:postgresql://${BLACKBOX_DB_HOST}:${BLACKBOX_DB_PORT}/benchbase?sslmode=disable\&ApplicationName=blackbox-fractured-read-probe}
  export BLACKBOX_COMPOSE_PROFILE BLACKBOX_DATABASE_SERVICE
  export BLACKBOX_DATABASE_CONTAINER BLACKBOX_CONNECTOR_SERVICE
  export BLACKBOX_CONNECTOR_CONTAINER BLACKBOX_DB_PORT
  export BLACKBOX_DB_USER BLACKBOX_DB_PASSWORD
  export BLACKBOX_BACKEND_SOURCE_ID
  export BLACKBOX_BENCHBASE_DATABASE_PROFILE BLACKBOX_JDBC_URL
}

blackbox_backend_prepare_run() {
  local compact_run_id=${BLACKBOX_ACTIVE_RUN_ID//-/}
  BLACKBOX_YSQL_SLOT_NAME="blackbox_${compact_run_id}_slot"
  BLACKBOX_YSQL_PUBLICATION_NAME="blackbox_${compact_run_id}_publication"
  BLACKBOX_ADAPTER_SCHEMA_FILE="$BLACKBOX_ROOT/yugabytedb_ysql/initdb/adapters/${BLACKBOX_BENCHMARK}.sql"
  [[ -f $BLACKBOX_ADAPTER_SCHEMA_FILE ]] ||
    blackbox_die "selected YugabyteDB adapter schema is missing: $BLACKBOX_ADAPTER_SCHEMA_FILE"
  BLACKBOX_YSQL_CAPTURE_TABLES=$(jq -er \
    '[.application_tables[], "benchbase.checker_run_fence", "benchbase.session_txn"] | join(",")' \
    <<<"$BLACKBOX_ADAPTER_CONTRACT") || return 1
  export BLACKBOX_YSQL_SLOT_NAME BLACKBOX_YSQL_PUBLICATION_NAME
  export BLACKBOX_YSQL_CAPTURE_TABLES BLACKBOX_ADAPTER_SCHEMA_FILE
  local -a sorted_capture_tables=()
  mapfile -t sorted_capture_tables < <(
    tr ',' '\n' <<<"$BLACKBOX_YSQL_CAPTURE_TABLES" | LC_ALL=C sort
  )
  local expected_membership_digest expected_replica_digest
  expected_membership_digest=$(
    printf '%s\n' "${sorted_capture_tables[@]}" | sha256sum | cut -d' ' -f1
  )
  expected_replica_digest=$(
    printf '%s\tf\n' "${sorted_capture_tables[@]}" | sha256sum | cut -d' ' -f1
  )
  # Consumed by the generic coordinator after this sourced driver returns.
  # shellcheck disable=SC2034
  BLACKBOX_BACKEND_MONITOR_ARGUMENTS=(
    --expected-backend-fact "publication_name=$BLACKBOX_YSQL_PUBLICATION_NAME"
    --expected-backend-fact "slot_name=$BLACKBOX_YSQL_SLOT_NAME"
    --expected-backend-fact "publication_membership_sha256=$expected_membership_digest"
    --expected-backend-fact "replica_identity_full_tables_sha256=$expected_replica_digest"
  )
}

blackbox_backend_require_commands() {
  :
}

blackbox_backend_teardown() {
  blackbox_compose down --volumes --remove-orphans
}

blackbox_backend_wait_for_schema_initialization() {
  local deadline=$(( $(date +%s) + ${BLACKBOX_STACK_TIMEOUT_SECONDS:-300} ))
  while (( $(date +%s) <= deadline )); do
    local status exit_code
    status=$("$BLACKBOX_DOCKER" inspect --format '{{.State.Status}}' \
      cdc-yugabytedb-ysql-init 2>/dev/null || true)
    exit_code=$("$BLACKBOX_DOCKER" inspect --format '{{.State.ExitCode}}' \
      cdc-yugabytedb-ysql-init 2>/dev/null || true)
    if [[ $status == exited && $exit_code == 0 ]]; then
      return 0
    fi
    if [[ $status == exited && $exit_code != 0 ]]; then
      blackbox_die "YugabyteDB schema/capture initialization failed"
      return 1
    fi
    sleep 1
  done
  blackbox_die "timed out waiting for YugabyteDB schema/capture initialization"
}

blackbox_backend_start_database_schema() {
  blackbox_compose up --detach --force-recreate redis \
    "$BLACKBOX_DATABASE_SERVICE" yugabytedb_ysql_init
  blackbox_backend_wait_for_schema_initialization
}

# Database plus schema only: initialization runs with capture-object creation
# disabled so no publication or slot exists in the evidence-disabled mode.
blackbox_backend_start_uninstrumented_database() {
  BLACKBOX_YSQL_CREATE_CAPTURE_OBJECTS=false blackbox_compose up --detach \
    --force-recreate "$BLACKBOX_DATABASE_SERVICE" yugabytedb_ysql_init
  blackbox_backend_wait_for_schema_initialization
}

blackbox_backend_verify_session_isolation() {
  local session_isolation
  session_isolation=$(blackbox_backend_database_query \
    "SELECT current_setting('default_transaction_isolation') || '|' || current_setting('yb_read_from_followers')") || return 1
  [[ ${session_isolation//[[:space:]]/} == "repeatableread|off" ]] || {
    blackbox_die "YugabyteDB session isolation contract differs"
    return 1
  }
}

blackbox_backend_start_connector() {
  blackbox_compose up --detach --no-deps --force-recreate \
    "$BLACKBOX_CONNECTOR_SERVICE"
}

blackbox_backend_admin_query() {
  blackbox_compose exec -T yugabytedb_ysql bin/ysqlsh \
    --host yugabytedb_ysql --username yugabyte --dbname benchbase \
    --no-psqlrc --set ON_ERROR_STOP=on --tuples-only --no-align \
    --field-separator=$'\t' --command "$1"
}

blackbox_backend_database() {
  local arguments=("$@")
  blackbox_compose exec -T \
    -e PGPASSWORD="$BLACKBOX_DB_PASSWORD" yugabytedb_ysql bin/ysqlsh \
    --host yugabytedb_ysql --username "$BLACKBOX_DB_USER" --dbname benchbase \
    --no-psqlrc --set ON_ERROR_STOP=on --tuples-only --no-align \
    --field-separator=$'\t' "${arguments[@]}"
}

blackbox_backend_database_query() {
  blackbox_backend_database --command "$1"
}

blackbox_backend_capture_schema_metadata() {
  blackbox_backend_admin_query "
    SELECT 'DATABASE', current_database(), pg_encoding_to_char(encoding), datcollate
      FROM pg_database WHERE datname = 'benchbase';
    SELECT 'TABLE', n.nspname || '.' || c.relname, c.relkind::text, c.relreplident::text
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'benchbase' AND c.relkind = 'r' ORDER BY c.relname;
    SELECT 'COLUMN', table_schema || '.' || table_name, column_name,
           ordinal_position, data_type, is_nullable,
           COALESCE(numeric_precision::text, '-'), COALESCE(numeric_scale::text, '-'),
           COALESCE(character_maximum_length::text, '-'),
           COALESCE(datetime_precision::text, '-'), '-', '-',
           CASE WHEN is_identity = 'YES' THEN 'identity' ELSE '-' END
      FROM information_schema.columns WHERE table_schema = 'benchbase'
     ORDER BY table_name, ordinal_position;
    SELECT 'PRIMARY', n.nspname || '.' || c.relname, a.attname, keys.ordinality
      FROM pg_constraint p JOIN pg_class c ON c.oid = p.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = keys.attnum
     WHERE p.contype = 'p' AND n.nspname = 'benchbase'
     ORDER BY c.relname, keys.ordinality"
}

blackbox_backend_capture_security_catalog() {
  blackbox_backend_admin_query "
    WITH security_lines AS (
      SELECT concat_ws(E'\\t', 'ROLE', rolname, rolsuper, rolinherit,
                       rolcreaterole, rolcreatedb, rolcanlogin, rolreplication,
                       rolbypassrls) AS line
        FROM pg_roles WHERE rolname !~ '^pg_'
      UNION ALL
      SELECT concat_ws(E'\\t', 'MEMBERSHIP', granted.rolname, member.rolname,
                       grantor.rolname, membership.admin_option)
        FROM pg_auth_members membership
        JOIN pg_roles granted ON granted.oid = membership.roleid
        JOIN pg_roles member ON member.oid = membership.member
        JOIN pg_roles grantor ON grantor.oid = membership.grantor
       WHERE granted.rolname !~ '^pg_' OR member.rolname !~ '^pg_'
      UNION ALL
      SELECT concat_ws(E'\\t', 'DATABASE', database.datname, owner.rolname,
                       COALESCE(array_to_string(database.datacl, ','), '-'))
        FROM pg_database database JOIN pg_roles owner ON owner.oid = database.datdba
       WHERE database.datname = 'benchbase'
      UNION ALL
      SELECT concat_ws(E'\\t', 'SCHEMA', namespace.nspname, owner.rolname,
                       COALESCE(array_to_string(namespace.nspacl, ','), '-'))
        FROM pg_namespace namespace JOIN pg_roles owner ON owner.oid = namespace.nspowner
       WHERE namespace.nspname = 'benchbase'
      UNION ALL
      SELECT concat_ws(E'\\t', 'TABLE', namespace.nspname || '.' || relation.relname,
                       owner.rolname,
                       COALESCE(array_to_string(relation.relacl, ','), '-'))
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        JOIN pg_roles owner ON owner.oid = relation.relowner
       WHERE namespace.nspname = 'benchbase' AND relation.relkind = 'r'
      UNION ALL
      SELECT concat_ws(E'\\t', 'PUBLICATION', publication.pubname, owner.rolname)
        FROM pg_publication publication
        JOIN pg_roles owner ON owner.oid = publication.pubowner
       WHERE publication.pubname = '$BLACKBOX_YSQL_PUBLICATION_NAME'
      UNION ALL
      SELECT concat_ws(E'\\t', 'DEFAULT_ACL', owner.rolname,
                       COALESCE(namespace.nspname, '-'), defaults.defaclobjtype,
                       array_to_string(defaults.defaclacl, ','))
        FROM pg_default_acl defaults
        JOIN pg_roles owner ON owner.oid = defaults.defaclrole
        LEFT JOIN pg_namespace namespace ON namespace.oid = defaults.defaclnamespace
       WHERE owner.rolname !~ '^pg_' OR namespace.nspname = 'benchbase'
      UNION ALL
      SELECT concat_ws(E'\\t', 'EFFECTIVE_DDL', role.rolname,
                       has_database_privilege(role.oid, 'benchbase', 'CREATE'),
                       has_schema_privilege(role.oid, 'benchbase', 'CREATE'))
        FROM pg_roles role WHERE role.rolcanlogin AND role.rolname !~ '^pg_'
    )
    SELECT line FROM security_lines ORDER BY line"
}

blackbox_backend_verify_schema() {
  blackbox_backend_capture_schema_metadata |
    python3 "$BLACKBOX_ROOT/scripts/backends/yugabytedb_ysql_catalog.py" \
      --benchmark "$BLACKBOX_BENCHMARK" || return 1
  local expected_tables actual_tables replica_tables publication_tables slot
  expected_tables=$(tr ',' '\n' <<<"$BLACKBOX_YSQL_CAPTURE_TABLES" | LC_ALL=C sort)
  actual_tables=$(blackbox_backend_admin_query \
    "SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY 1") || return 1
  [[ $actual_tables == "$expected_tables" ]] || {
    blackbox_die "YugabyteDB table roster differs from selected adapter"
    return 1
  }
  replica_tables=$(blackbox_backend_admin_query \
    "SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' AND c.relreplident='f' ORDER BY 1") || return 1
  [[ $replica_tables == "$expected_tables" ]] || {
    blackbox_die "YugabyteDB REPLICA IDENTITY FULL roster differs"
    return 1
  }
  publication_tables=$(blackbox_backend_admin_query \
    "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname='$BLACKBOX_YSQL_PUBLICATION_NAME' ORDER BY 1") || return 1
  [[ $publication_tables == "$expected_tables" ]] || {
    blackbox_die "YugabyteDB publication membership differs"
    return 1
  }
  slot=$(blackbox_backend_admin_query \
    "SELECT slot_name || E'\\t' || plugin || E'\\t' || database || E'\\t' || yb_lsn_type FROM pg_replication_slots WHERE slot_name='$BLACKBOX_YSQL_SLOT_NAME'") || return 1
  [[ $slot == "$BLACKBOX_YSQL_SLOT_NAME"$'\tpgoutput\tbenchbase\tSEQUENCE' ]] || {
    blackbox_die "YugabyteDB logical slot contract differs"
    return 1
  }
  local capture_counts
  capture_counts=$(blackbox_backend_admin_query \
    "SELECT (SELECT count(*) FROM pg_publication) || E'\\t' || (SELECT count(*) FROM pg_replication_slots)") || return 1
  [[ $capture_counts == $'1\t1' ]] || {
    blackbox_die "YugabyteDB requires exactly one publication and one slot"
    return 1
  }
  local session_settings
  session_settings=$(blackbox_backend_database_query \
    "SELECT current_setting('client_encoding') || E'\\t' || current_setting('TimeZone') || E'\\t' || current_setting('search_path') || E'\\t' || current_setting('default_transaction_isolation') || E'\\t' || current_setting('yb_read_from_followers')") || return 1
  [[ $session_settings == $'UTF8\tUTC\tbenchbase\trepeatable read\toff' ]] || {
    blackbox_die "YugabyteDB effective session settings differ"
    return 1
  }
  local container_command
  container_command=$("$BLACKBOX_DOCKER" inspect --format '{{json .Config.Cmd}}' \
    "$BLACKBOX_DATABASE_CONTAINER") || return 1
  [[ $container_command == *"cdc_enable_intra_transactional_before_image=true"* &&
     $container_command == *"cdc_send_null_before_image_if_not_exists=false"* ]] || {
    blackbox_die "YugabyteDB before-image flags differ"
    return 1
  }
  local role_contract_mismatches ownership_mismatches privilege_mismatches
  local membership_mismatches unauthorized_ddl_logins
  role_contract_mismatches=$(blackbox_backend_admin_query "
    WITH expected(role_name) AS (
      VALUES ('blackbox_setup'), ('blackbox_application'), ('blackbox_replication')
    )
    SELECT count(*) FROM expected
    LEFT JOIN pg_roles role ON role.rolname = expected.role_name
    WHERE role.oid IS NULL OR NOT (
      role.rolcanlogin AND NOT role.rolsuper AND NOT role.rolcreaterole AND
      NOT role.rolcreatedb AND NOT role.rolbypassrls AND
      CASE WHEN expected.role_name = 'blackbox_replication'
           THEN role.rolreplication ELSE NOT role.rolreplication END
    )") || return 1
  ownership_mismatches=$(blackbox_backend_admin_query "
    SELECT count(*) FROM (
      SELECT owner.rolname AS owner_name
        FROM pg_database database JOIN pg_roles owner ON owner.oid = database.datdba
       WHERE database.datname = 'benchbase'
      UNION ALL
      SELECT owner.rolname
        FROM pg_namespace namespace JOIN pg_roles owner ON owner.oid = namespace.nspowner
       WHERE namespace.nspname = 'benchbase'
      UNION ALL
      SELECT owner.rolname
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        JOIN pg_roles owner ON owner.oid = relation.relowner
       WHERE namespace.nspname = 'benchbase' AND relation.relkind = 'r'
    ) owned WHERE owner_name <> 'blackbox_setup'") || return 1
  privilege_mismatches=$(blackbox_backend_admin_query "
    SELECT CASE WHEN
      has_schema_privilege('blackbox_application', 'benchbase', 'USAGE') AND
      NOT has_schema_privilege('blackbox_application', 'benchbase', 'CREATE') AND
      has_schema_privilege('blackbox_replication', 'benchbase', 'USAGE') AND
      NOT has_schema_privilege('blackbox_replication', 'benchbase', 'CREATE') AND
      NOT has_database_privilege('blackbox_application', 'benchbase', 'CREATE') AND
      NOT has_database_privilege('blackbox_replication', 'benchbase', 'CREATE') AND
      NOT EXISTS (
        SELECT 1 FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'benchbase' AND relation.relkind = 'r' AND NOT (
          has_table_privilege('blackbox_application', relation.oid, 'SELECT') AND
          has_table_privilege('blackbox_application', relation.oid, 'INSERT') AND
          has_table_privilege('blackbox_application', relation.oid, 'UPDATE') AND
          has_table_privilege('blackbox_application', relation.oid, 'DELETE') AND
          NOT has_table_privilege('blackbox_application', relation.oid, 'TRUNCATE') AND
          NOT has_table_privilege('blackbox_application', relation.oid, 'REFERENCES') AND
          NOT has_table_privilege('blackbox_application', relation.oid, 'TRIGGER') AND
          has_table_privilege('blackbox_replication', relation.oid, 'SELECT') AND
          NOT has_table_privilege('blackbox_replication', relation.oid, 'INSERT') AND
          NOT has_table_privilege('blackbox_replication', relation.oid, 'UPDATE') AND
          NOT has_table_privilege('blackbox_replication', relation.oid, 'DELETE') AND
          NOT has_table_privilege('blackbox_replication', relation.oid, 'TRUNCATE') AND
          NOT has_table_privilege('blackbox_replication', relation.oid, 'REFERENCES') AND
          NOT has_table_privilege('blackbox_replication', relation.oid, 'TRIGGER')
        )
      ) THEN 0 ELSE 1 END") || return 1
  membership_mismatches=$(blackbox_backend_admin_query "
    SELECT count(*) FROM pg_auth_members membership
    JOIN pg_roles granted ON granted.oid = membership.roleid
    JOIN pg_roles member ON member.oid = membership.member
    WHERE granted.rolname IN ('blackbox_setup','blackbox_application','blackbox_replication')
       OR member.rolname IN ('blackbox_setup','blackbox_application','blackbox_replication')") || return 1
  unauthorized_ddl_logins=$(blackbox_backend_admin_query "
    WITH RECURSIVE membership(member, granted_role) AS (
      SELECT member, roleid FROM pg_auth_members
      UNION
      SELECT membership.member, inherited.roleid
        FROM membership JOIN pg_auth_members inherited
          ON inherited.member = membership.granted_role
    ), ddl_roles(role_id) AS (
      SELECT role.oid FROM pg_roles role WHERE
        role.rolsuper OR role.rolcreaterole OR role.rolcreatedb OR
        has_database_privilege(role.oid, 'benchbase', 'CREATE') OR
        has_schema_privilege(role.oid, 'benchbase', 'CREATE') OR
        EXISTS (SELECT 1 FROM pg_database database
                WHERE database.datname = 'benchbase' AND database.datdba = role.oid) OR
        EXISTS (SELECT 1 FROM pg_namespace namespace
                WHERE namespace.nspname = 'benchbase' AND namespace.nspowner = role.oid) OR
        EXISTS (SELECT 1 FROM pg_class relation
                JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'benchbase' AND relation.relkind = 'r'
                  AND relation.relowner = role.oid)
    ), capable_logins(role_id) AS (
      SELECT role.oid FROM pg_roles role
       WHERE role.rolcanlogin AND (
         role.oid IN (SELECT role_id FROM ddl_roles) OR
         EXISTS (SELECT 1 FROM membership
                  WHERE membership.member = role.oid
                    AND membership.granted_role IN (SELECT role_id FROM ddl_roles))
       )
    )
    SELECT count(*) FROM capable_logins
    JOIN pg_roles role ON role.oid = capable_logins.role_id
    WHERE role.rolname NOT IN ('yugabyte','postgres','blackbox_setup')") || return 1
  if [[ ${role_contract_mismatches//[[:space:]]/} != 0 ||
        ${ownership_mismatches//[[:space:]]/} != 0 ||
        ${privilege_mismatches//[[:space:]]/} != 0 ||
        ${membership_mismatches//[[:space:]]/} != 0 ||
        ${unauthorized_ddl_logins//[[:space:]]/} != 0 ]]; then
    blackbox_die \
      "YugabyteDB dedicated role/ownership/DDL privilege contract differs: role=$role_contract_mismatches ownership=$ownership_mismatches privileges=$privilege_mismatches membership=$membership_mismatches unauthorized_ddl_logins=$unauthorized_ddl_logins"
    return 1
  fi
}

blackbox_backend_count_table() {
  local table=$1
  [[ $table =~ ^benchbase\.([a-z][a-z0-9_]*)$ ]] || return 1
  blackbox_backend_database_query "SELECT COUNT(*) FROM benchbase.\"${BASH_REMATCH[1]}\""
}

blackbox_backend_commit_fence() {
  local run_id=$1 phase=$2 nonce=$3
  blackbox_backend_database_query \
    "BEGIN; INSERT INTO benchbase.checker_run_fence (run_id, phase, nonce) VALUES ('$run_id', '$phase', '$nonce'); COMMIT;" >/dev/null
}

blackbox_backend_active_external_connections() {
  blackbox_backend_admin_query "
    WITH RECURSIVE membership(member, granted_role) AS (
      SELECT member, roleid FROM pg_auth_members
      UNION
      SELECT membership.member, inherited.roleid
        FROM membership JOIN pg_auth_members inherited
          ON inherited.member = membership.granted_role
    ), ddl_roles(role_id) AS (
      SELECT role.oid FROM pg_roles role WHERE
        role.rolsuper OR role.rolcreaterole OR role.rolcreatedb OR
        has_database_privilege(role.oid, 'benchbase', 'CREATE') OR
        has_schema_privilege(role.oid, 'benchbase', 'CREATE') OR
        EXISTS (SELECT 1 FROM pg_database database
                WHERE database.datname = 'benchbase' AND database.datdba = role.oid) OR
        EXISTS (SELECT 1 FROM pg_namespace namespace
                WHERE namespace.nspname = 'benchbase' AND namespace.nspowner = role.oid) OR
        EXISTS (SELECT 1 FROM pg_class relation
                JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'benchbase' AND relation.relkind = 'r'
                  AND relation.relowner = role.oid)
    ), capable_logins(role_id) AS (
      SELECT role.oid FROM pg_roles role
       WHERE role.rolcanlogin AND (
         role.oid IN (SELECT role_id FROM ddl_roles) OR
         EXISTS (SELECT 1 FROM membership
                  WHERE membership.member = role.oid
                    AND membership.granted_role IN (SELECT role_id FROM ddl_roles))
       )
    )
    SELECT count(*) FROM pg_stat_activity activity
    WHERE activity.pid <> pg_backend_pid() AND
      activity.backend_type = 'client backend' AND (
      activity.usesysid IN (SELECT role_id FROM capable_logins) OR
      (activity.datname = 'benchbase' AND
       activity.usename <> 'blackbox_replication')
    )"
}

blackbox_backend_wait_for_connector_progress() {
  local active
  active=$(blackbox_backend_admin_query \
    "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$BLACKBOX_YSQL_SLOT_NAME' AND plugin='pgoutput' AND database='benchbase' AND yb_lsn_type='SEQUENCE' AND active AND active_pid IS NOT NULL") || return 1
  [[ ${active//[[:space:]]/} == 1 ]] || return 1
  local logs
  logs=$("$BLACKBOX_DOCKER" logs "$BLACKBOX_CONNECTOR_CONTAINER" 2>&1 || true)
  for expected in \
    'Starting streaming' \
    'connector.class = io.debezium.connector.postgresql.YugabyteDBConnector' \
    'slot.lsn.type = SEQUENCE' \
    'plugin.name = pgoutput' \
    'task.id = 0' \
    'Connector config provide.transaction.metadata is set to true'; do
    grep -q "$expected" <<<"$logs" || return 1
  done
  local connector_checksum connector_count stock_count
  connector_checksum=$("$BLACKBOX_DOCKER" exec "$BLACKBOX_CONNECTOR_CONTAINER" \
    sh -lc "sha256sum /debezium/lib/yugabytedb-source-connector-dz.2.5.2.yb.2026.1.jar | cut -d' ' -f1") || return 1
  [[ $connector_checksum == 4e83132cbebdec53e553f69d4cfdbe48dac563105aabc5db4b880cdcf0118d98 ]] || return 1
  connector_count=$("$BLACKBOX_DOCKER" exec "$BLACKBOX_CONNECTOR_CONTAINER" \
    sh -lc "find /debezium/lib -maxdepth 1 -name 'yugabytedb-source-connector-*.jar' | wc -l") || return 1
  stock_count=$("$BLACKBOX_DOCKER" exec "$BLACKBOX_CONNECTOR_CONTAINER" \
    sh -lc "find /debezium/lib -maxdepth 1 -name 'debezium-connector-postgres-*.jar' | wc -l") || return 1
  [[ ${connector_count//[[:space:]]/} == 1 &&
     ${stock_count//[[:space:]]/} == 0 ]]
}

blackbox_backend_capture_attestation_inputs() {
  local tables_file=$1 facts_file=$2 metadata_file
  metadata_file=$(mktemp /tmp/blackbox-yugabyte-schema.XXXXXX) || return 1
  blackbox_backend_capture_schema_metadata >"$metadata_file" || return 1
  python3 "$BLACKBOX_ROOT/scripts/backends/yugabytedb_ysql_catalog.py" \
    --benchmark "$BLACKBOX_BENCHMARK" --emit-tables \
    <"$metadata_file" >"$tables_file" || {
      rm -f -- "$metadata_file"
      return 1
    }
  local image_digest membership_digest replica_digest types_digest privileges_digest
  image_digest=$("$BLACKBOX_DOCKER" inspect --format '{{.Image}}' \
    "$BLACKBOX_DATABASE_CONTAINER") || return 1
  image_digest=${image_digest#sha256:}
  membership_digest=$(blackbox_backend_admin_query \
    "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname='$BLACKBOX_YSQL_PUBLICATION_NAME' ORDER BY 1" |
    sha256sum | cut -d' ' -f1) || return 1
  replica_digest=$(blackbox_backend_admin_query \
    "SELECT n.nspname || '.' || c.relname || E'\\t' || c.relreplident::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY 1" |
    sha256sum | cut -d' ' -f1) || return 1
  types_digest=$(blackbox_backend_admin_query \
    "SELECT c.oid || E'\\t' || n.nspname || '.' || c.relname || E'\\t' || a.attnum || E'\\t' || a.attname || E'\\t' || a.atttypid || E'\\t' || a.atttypmod || E'\\t' || a.attnotnull FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid WHERE n.nspname='benchbase' AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum" |
    sha256sum | cut -d' ' -f1) || return 1
  privileges_digest=$(blackbox_backend_capture_security_catalog |
    sha256sum | cut -d' ' -f1) || return 1
  local session_settings client_encoding time_zone search_path isolation follower_reads
  session_settings=$(blackbox_backend_database_query \
    "SELECT current_setting('client_encoding') || E'\\t' || current_setting('TimeZone') || E'\\t' || current_setting('search_path') || E'\\t' || current_setting('default_transaction_isolation') || E'\\t' || current_setting('yb_read_from_followers')") || return 1
  IFS=$'\t' read -r client_encoding time_zone search_path isolation follower_reads \
    <<<"$session_settings"
  case "$follower_reads" in
    off|false) follower_reads=false ;;
    on|true) follower_reads=true ;;
    *) return 1 ;;
  esac
  local slot_facts observed_slot observed_plugin observed_database observed_lsn
  slot_facts=$(blackbox_backend_admin_query \
    "SELECT slot_name || E'\\t' || plugin || E'\\t' || database || E'\\t' || yb_lsn_type FROM pg_replication_slots WHERE slot_name='$BLACKBOX_YSQL_SLOT_NAME'") || return 1
  IFS=$'\t' read -r observed_slot observed_plugin observed_database observed_lsn \
    <<<"$slot_facts"
  local build_text server_version server_build_hash ysql_server_version
  local container_command intra_before null_before
  build_text=$("$BLACKBOX_DOCKER" exec "$BLACKBOX_DATABASE_CONTAINER" \
    bin/yb-admin --version) || return 1
  server_version=$(sed -n 's/^version \([^ ]*\) build \([^ ]*\).*/\1-b\2/p' \
    <<<"$build_text")
  server_build_hash=$(sed -n 's/.* revision \([0-9a-f]\{40\}\) .*/\1/p' \
    <<<"$build_text")
  ysql_server_version=$(blackbox_backend_admin_query "SELECT version()") || return 1
  container_command=$("$BLACKBOX_DOCKER" inspect --format '{{json .Config.Cmd}}' \
    "$BLACKBOX_DATABASE_CONTAINER") || return 1
  intra_before=false
  null_before=true
  [[ $container_command == *"cdc_enable_intra_transactional_before_image=true"* ]] &&
    intra_before=true
  [[ $container_command == *"cdc_send_null_before_image_if_not_exists=false"* ]] &&
    null_before=false
  jq -n --arg publication "$BLACKBOX_YSQL_PUBLICATION_NAME" --arg image "$image_digest" \
    --arg membership "$membership_digest" --arg replica "$replica_digest" \
    --arg types "$types_digest" --arg privileges "$privileges_digest" \
    --arg server_version "$server_version" --arg server_build_hash "$server_build_hash" \
    --arg ysql_server_version "$ysql_server_version" \
    --arg client_encoding "$client_encoding" --arg time_zone "$time_zone" \
    --arg search_path "$search_path" --arg isolation "$isolation" \
    --arg follower_reads "$follower_reads" --arg observed_slot "$observed_slot" \
    --arg observed_plugin "$observed_plugin" --arg observed_database "$observed_database" \
    --arg observed_lsn "$observed_lsn" \
    --arg intra_before "$intra_before" --arg null_before "$null_before" '
    [
      {name:"database_name",canonical_value:"benchbase"},
      {name:"schema_name",canonical_value:"benchbase"},
      {name:"server_version",canonical_value:$server_version},
      {name:"server_build_hash",canonical_value:$server_build_hash},
      {name:"ysql_server_version",canonical_value:$ysql_server_version},
      {name:"client_encoding",canonical_value:$client_encoding},
      {name:"time_zone",canonical_value:$time_zone},
      {name:"search_path",canonical_value:$search_path},
      {name:"default_transaction_isolation",canonical_value:$isolation},
      {name:"yb_read_from_followers",canonical_value:$follower_reads},
      {name:"publication_name",canonical_value:$publication},
      {name:"slot_name",canonical_value:$observed_slot},
      {name:"slot_database",canonical_value:$observed_database},
      {name:"slot_plugin",canonical_value:$observed_plugin},
      {name:"slot_lsn_type",canonical_value:$observed_lsn},
      {name:"streaming_mode",canonical_value:"default"},
      {name:"connector_class",canonical_value:"io.debezium.connector.postgresql.YugabyteDBConnector"},
      {name:"connector_source",canonical_value:"postgresql"},
      {name:"connector_version",canonical_value:"dz.2.5.2.yb.2026.1"},
      {name:"connector_sha256",canonical_value:"4e83132cbebdec53e553f69d4cfdbe48dac563105aabc5db4b880cdcf0118d98"},
      {name:"connector_tasks",canonical_value:"1"},
      {name:"redis_stream",canonical_value:"cdc"},
      {name:"cdc_enable_intra_transactional_before_image",canonical_value:$intra_before},
      {name:"cdc_send_null_before_image_if_not_exists",canonical_value:$null_before},
      {name:"database_image_digest",canonical_value:$image},
      {name:"publication_membership_sha256",canonical_value:$membership},
      {name:"replica_identity_full_tables_sha256",canonical_value:$replica},
      {name:"table_oid_type_typmod_sha256",canonical_value:$types},
      {name:"dedicated_privileges_sha256",canonical_value:$privileges}
    ]' >"$facts_file"
  rm -f -- "$metadata_file"
}

blackbox_backend_status() {
  blackbox_backend_admin_query \
    "SELECT slot_name, plugin, database, yb_lsn_type, active, confirmed_flush_lsn FROM pg_replication_slots ORDER BY slot_name; SELECT pubname, schemaname, tablename FROM pg_publication_tables ORDER BY 1,2,3; SELECT count(*) AS tablets FROM yb_local_tablets"
}
