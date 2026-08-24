#!/usr/bin/env bash
# Live TPC-C value-normalization capture on the pinned production
# YugabyteDB YSQL stack: boundary corpus + write shapes, raw Redis CDC,
# bootstrap scan, JDBC capture, C++ codec replay, and verification.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
repo_root=$(cd -- "$script_dir/../.." && pwd)
readonly repo_root
capture_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
readonly capture_timestamp
readonly output_dir=${1:-"$script_dir/fixtures/pgoutput-tpcc_value_normalization-${capture_timestamp}"}
readonly cdc_codec_replay_bin=${BLACKBOX_CDC_CODEC_REPLAY_BIN:?set BLACKBOX_CDC_CODEC_REPLAY_BIN}
readonly stack_timeout_seconds=${BLACKBOX_CAPTURE_STACK_TIMEOUT_SECONDS:-300}

if [[ -e "$output_dir" ]]; then
  echo "capture output already exists: $output_dir" >&2
  exit 2
fi
if [[ ! -x "$cdc_codec_replay_bin" ]]; then
  echo "BLACKBOX_CDC_CODEC_REPLAY_BIN must name the built codec replay tool" >&2
  exit 2
fi

adapter_tables=$(jq -er '[.application_tables[]] | join(",")' \
  "$repo_root/protocol/adapters/tpcc.json")
capture_tables="$adapter_tables,benchbase.checker_run_fence,benchbase.session_txn"

compose() {
  BLACKBOX_ADAPTER_SCHEMA_FILE="$repo_root/yugabytedb_ysql/initdb/adapters/tpcc.sql" \
  BLACKBOX_YSQL_CAPTURE_TABLES="$capture_tables" \
    docker compose --file "$repo_root/docker-compose.yml" \
      --profile yugabytedb_ysql "$@"
}

ysql_application() {
  docker exec --interactive \
    --env PGPASSWORD=blackbox_application_password \
    cdc-yugabytedb-ysql bin/ysqlsh \
    --host yugabytedb_ysql --username blackbox_application --dbname benchbase \
    --no-psqlrc --set ON_ERROR_STOP=on "$@"
}

ysql_admin() {
  docker exec --interactive cdc-yugabytedb-ysql bin/ysqlsh \
    --host yugabytedb_ysql --username yugabyte --dbname benchbase \
    --no-psqlrc --set ON_ERROR_STOP=on "$@"
}

cleanup() {
  local status=$?
  trap - EXIT
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  return "$status"
}
trap cleanup EXIT

mkdir -p -- "$output_dir"

compose down --volumes --remove-orphans >/dev/null 2>&1 || true
compose up --detach --force-recreate redis yugabytedb_ysql yugabytedb_ysql_init

deadline=$(( $(date +%s) + stack_timeout_seconds ))
while :; do
  state=$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' \
    cdc-yugabytedb-ysql-init 2>/dev/null || echo "missing -1")
  if [[ $state == "exited 0" ]]; then
    break
  fi
  if [[ $state == exited* ]]; then
    echo "schema/capture initialization failed: $state" >&2
    docker logs cdc-yugabytedb-ysql-init >&2 || true
    exit 1
  fi
  if (( $(date +%s) > deadline )); then
    echo "timed out waiting for schema/capture initialization" >&2
    exit 1
  fi
  sleep 1
done

compose up --detach --no-deps debezium_yugabytedb_ysql
deadline=$(( $(date +%s) + stack_timeout_seconds ))
until docker logs cdc-debezium-yugabytedb-ysql 2>&1 | grep -q 'Starting streaming'; do
  if (( $(date +%s) > deadline )); then
    echo "timed out waiting for connector streaming" >&2
    docker logs cdc-debezium-yugabytedb-ysql >&2 || true
    exit 1
  fi
  sleep 1
done

ysql_application --file - \
  <"$script_dir/workload/tpcc_value_normalization.sql" \
  >"$output_dir/workload-corpus.log" 2>&1
ysql_application --file - \
  <"$script_dir/workload/tpcc_write_shapes.sql" \
  >"$output_dir/workload-shapes.log" 2>&1

# Wait until the connector has delivered both marker-terminated transactions.
deadline=$(( $(date +%s) + stack_timeout_seconds ))
until [[ $(docker exec cdc-redis redis-cli --raw XRANGE cdc - + \
    | grep -c '"status":"END"' || true) -ge 2 ]]; do
  if (( $(date +%s) > deadline )); then
    echo "timed out waiting for CDC transaction delivery" >&2
    exit 1
  fi
  sleep 1
done
sleep 2

docker exec cdc-redis redis-cli --json XRANGE cdc - + >"$output_dir/redis-stream.json"
docker exec cdc-redis redis-cli --raw XRANGE cdc - + >"$output_dir/redis-stream.raw.txt"

# Bootstrap scan through the registered backend SQL renderer.
python3 - "$repo_root" >"$output_dir/bootstrap-scan-queries.sql" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
sys.path.insert(0, str(root / "protocol" / "tools"))
from backend_sql_renderer import BackendSqlRendererRegistry
from bootstrap_adapters.tpcc import TpccBootstrapAdapter

adapter = json.loads((root / "protocol" / "adapters" / "tpcc.json").read_text())
plan = TpccBootstrapAdapter().build_queries(adapter)
registry = BackendSqlRendererRegistry.discover(
    root / "protocol" / "tools" / "backend_sql_renderers"
)
for query in registry.require("yugabytedb_ysql").bootstrap_queries(adapter, plan):
    print(query["sql"] + ";")
PY
ysql_application --tuples-only --no-align --file - \
  <"$output_dir/bootstrap-scan-queries.sql" >"$output_dir/bootstrap-scan.jsonl"

# JDBC capture through the registered Java backend adapter and shared codecs.
jdbc_build_dir=$(mktemp -d /tmp/yugabytedb-tpcc-normalization-jdbc-build.XXXXXX)
mvn --file "$repo_root/../benchbase/pom.xml" --batch-mode --quiet \
  --activate-profiles postgres -Dmaven.gitcommitid.skip=true -Dfmt.skip \
  -DbuildDirectory="$jdbc_build_dir" -DskipTests clean compile \
  >"$output_dir/jdbc-build.log" 2>&1
BLACKBOX_BACKEND=yugabytedb_ysql \
BLACKBOX_JDBC_URL="jdbc:postgresql://host.docker.internal:5433/benchbase?sslmode=disable" \
BLACKBOX_DB_USER=blackbox_application \
BLACKBOX_DB_PASSWORD=blackbox_application_password \
  mvn --file "$repo_root/../benchbase/pom.xml" --batch-mode --quiet \
    --activate-profiles postgres -Dmaven.gitcommitid.skip=true -Dfmt.skip \
    -DbuildDirectory="$jdbc_build_dir" \
    -Dexec.executable=java -Dexec.classpathScope=runtime \
    "-Dexec.args=-classpath %classpath com.oltpbenchmark.blackbox.tools.TpccJdbcNormalizationCapture" \
    org.codehaus.mojo:exec-maven-plugin:3.4.1:exec \
    >"$output_dir/jdbc-capture.json" 2>"$output_dir/jdbc-capture.log"
rm -rf -- "$jdbc_build_dir"

"$cdc_codec_replay_bin" \
  --input "$output_dir/redis-stream.json" \
  --backend yugabytedb_ysql \
  --benchmark tpcc \
  --backend-source-id yugabytedb_ysql_main \
  >"$output_dir/cpp-codec-replay.json"
cp -- "$cdc_codec_replay_bin" "$output_dir/blackbox-cdc-codec-replay"

# Provenance and stack evidence.
ysql_admin --echo-all --command \
  "SELECT version();
   SELECT slot_name, plugin, database, yb_lsn_type, active FROM pg_replication_slots ORDER BY slot_name;
   SELECT pubname, schemaname, tablename FROM pg_publication_tables ORDER BY 1,2,3;
   SELECT n.nspname, c.relname, c.relreplident::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='benchbase' AND c.relkind='r' ORDER BY c.relname;" \
  >"$output_dir/database-catalog.txt" 2>&1
docker exec cdc-yugabytedb-ysql bin/yb-admin --version \
  >"$output_dir/database-build.txt"
docker exec cdc-debezium-yugabytedb-ysql sh -lc \
  'sha256sum /debezium/lib/yugabytedb-source-connector-*.jar' \
  >"$output_dir/connector-identity.txt"
docker logs --timestamps cdc-debezium-yugabytedb-ysql \
  >"$output_dir/debezium-server.log" 2>&1
docker logs --timestamps cdc-yugabytedb-ysql-init \
  >"$output_dir/initialize.log" 2>&1
compose config >"$output_dir/docker-compose-config.yml"
docker image inspect \
  "$(jq -er .artifacts.database.image "$script_dir/provenance.lock.json")" \
  >"$output_dir/database-image.json"
docker image inspect db-monitoring/yugabytedb-ysql:dz.2.5.2.yb.2026.1 \
  >"$output_dir/connector-image.json"
cp -- "$script_dir/provenance.lock.json" "$output_dir/provenance.lock.json"
cp -- "$repo_root/yugabytedb_ysql/initdb/adapters/tpcc.sql" "$output_dir/adapter-schema.sql"
cp -- "$script_dir/workload/tpcc_value_normalization.sql" "$output_dir/"
cp -- "$script_dir/workload/tpcc_write_shapes.sql" "$output_dir/"
cp -- "$script_dir/verify_tpcc_value_normalization.py" "$output_dir/"

python3 "$script_dir/verify_tpcc_value_normalization.py" \
  --redis-stream "$output_dir/redis-stream.json" \
  --bootstrap-scan "$output_dir/bootstrap-scan.jsonl" \
  --jdbc-capture "$output_dir/jdbc-capture.json" \
  --cpp-codec-replay "$output_dir/cpp-codec-replay.json" \
  --report "$output_dir/normalization-report.json"

(
  cd "$output_dir"
  find . -type f ! -name SHA256SUMS -print0 | LC_ALL=C sort -z | xargs -0 sha256sum
) >"$output_dir/SHA256SUMS"
(cd "$output_dir" && sha256sum -c SHA256SUMS >/dev/null)
echo "TPC-C normalization capture complete: $output_dir"
