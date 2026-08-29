#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
evidence=${1:-"$root/evidence/oceanbase-mysql-capability-$timestamp"}
source_container=${OCEANBASE_MYSQL_SOURCE_CONTAINER:-oceanbase-mysql-capability-source}
debezium_container=${OCEANBASE_MYSQL_DEBEZIUM_CONTAINER:-oceanbase-mysql-capability-debezium}
redis_container=${OCEANBASE_MYSQL_REDIS_CONTAINER:-oceanbase-mysql-capability-redis}
network_name=${OCEANBASE_MYSQL_NETWORK:-oceanbase-mysql-capability}
application_properties=${OCEANBASE_MYSQL_APPLICATION_PROPERTIES:-"$root/application.properties"}
cluster_name=${OCEANBASE_MYSQL_CLUSTER_NAME:-oceanbase_contract}

mkdir -p "$evidence/configuration" "$evidence/logs"
cp "$application_properties" "$evidence/configuration/application.properties"
cp "$root/docker-compose.yml" "$evidence/configuration/"
cp "$root/provenance.lock.json" "$evidence/configuration/"
cp -R "$root/setup" "$evidence/configuration/"

"$root/verify_artifacts.sh" >"$evidence/artifact-verification.txt"

docker inspect "$source_container" "$debezium_container" "$redis_container" \
  >"$evidence/container-inspect.json"
docker network inspect "$network_name" \
  >"$evidence/network-inspect.json"
docker logs "$source_container" >"$evidence/logs/oceanbase-source.log" 2>&1
docker logs "$debezium_container" >"$evidence/debezium-server.log" 2>&1
docker logs "$redis_container" >"$evidence/logs/redis.log" 2>&1

docker exec "$source_container" bash -lc '
  /home/admin/oceanbase/bin/observer --version 2>&1 | sed -n "1,8p"
  /home/admin/obproxy/bin/obproxy -V 2>&1 | sed -n "1,5p"
  /home/ds/oblogproxy/bin/logproxy --version 2>&1
  rpm -q oceanbase-ce oceanbase-ce-libs obproxy-ce obbinlog-ce
' >"$evidence/runtime-versions.txt"
printf 'debezium-server-dist 3.6.1.Final\n' >>"$evidence/runtime-versions.txt"

capture_query() {
  local sql=$1 output=$2 command_output exit_code data_rows
  set +e
  command_output=$(docker exec "$source_container" obclient -A -c \
    -h127.0.0.1 -P2883 -ublackbox_connector@blackbox -pOceanBaseConnector_2026! \
    -e "$sql" 2>&1)
  exit_code=$?
  set -e
  data_rows=$(awk 'NR > 1 && NF {count++} END {print count + 0}' <<<"$command_output")
  {
    printf 'query=%s\n' "$sql"
    printf 'exit_code=%s\n' "$exit_code"
    printf 'data_row_count=%s\n' "$data_rows"
    printf '%s\n' "$command_output"
  } >"$output"
}

capture_query 'SHOW MASTER STATUS;' "$evidence/show-master-status.txt"
capture_query 'SHOW BINARY LOG STATUS;' "$evidence/show-binary-log-status.txt"

docker exec "$source_container" obclient -A -c -h127.0.0.1 -P2983 \
  -e "SHOW BINLOG SERVER FOR TENANT \`$cluster_name\`.\`blackbox\`; SHOW BINLOG STATUS FOR TENANT \`$cluster_name\`.\`blackbox\`;" \
  >"$evidence/official-binlog-status.txt"

{
  printf 'length='
  docker exec "$redis_container" redis-cli XLEN cdc
  docker exec "$redis_container" redis-cli XINFO STREAM cdc 2>&1 || true
} >"$evidence/redis-cdc-info.txt"
docker exec "$redis_container" redis-cli --json XRANGE cdc - + COUNT 1000 \
  >"$evidence/raw-redis-cdc.json"

docker run --rm --entrypoint /bin/bash \
  quay.io/debezium/server@sha256:11124f362db7c99096ead22237374075f42737c68487cb790e69bdc4ce2e817f \
  -lc 'javap -classpath /debezium/lib/debezium-connector-mysql-3.6.1.Final.jar -c -p io.debezium.connector.mysql.jdbc.MySqlConnection' \
  >"$evidence/debezium-mysql-connection-bytecode.txt"

docker exec -i "$source_container" obclient -h127.0.0.1 -P2881 \
  -uroot@blackbox -pOceanBaseContract_2026 -A \
  <"$root/setup/capture-catalog.sql" >"$evidence/catalog-attestation.txt"

python3 "$root/verify_capability.py" "$evidence" \
  >"$evidence/verifier-output.json"

(
  cd "$evidence"
  checksum_file=$(mktemp /tmp/oceanbase-mysql-capability-checksums.XXXXXX)
  find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum \
    >"$checksum_file"
  mv "$checksum_file" SHA256SUMS
  sha256sum --check SHA256SUMS >/dev/null
)

printf '%s\n' "$evidence"
