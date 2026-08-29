#!/usr/bin/env bash
set -euo pipefail

source_image=db-monitoring/oceanbase-obbinlog-contract:4.4.2.1-obbinlog4.3.6.4
capability_source_image=db-monitoring/oceanbase-mysql-capability-source:4.4.2.1-obbinlog4.3.6.4
debezium_image=quay.io/debezium/server@sha256:11124f362db7c99096ead22237374075f42737c68487cb790e69bdc4ce2e817f
redis_image=redis@sha256:9702d01c1f10c3ea9f48211b4362e44f154ff02d063e6f7268eba804059f53bf

require_equal() {
  local name=$1 actual=$2 expected=$3
  if [[ $actual != "$expected" ]]; then
    printf '%s mismatch: expected %s, got %s\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
  printf '%s=%s\n' "$name" "$actual"
}

require_equal source_image_id \
  "$(docker image inspect "$source_image" --format '{{.Id}}')" \
  sha256:26eef9c42eeab6f49ae9035aa9cb75317f42839e03ef46ee667293fbf7336661
require_equal capability_source_image_id \
  "$(docker image inspect "$capability_source_image" --format '{{.Id}}')" \
  sha256:0a46bf5d1cd19bf74ed8f1da746cd140971ddb28b03a2ecf8cd6f159e01ade0a
require_equal debezium_image_id \
  "$(docker image inspect "$debezium_image" --format '{{.Id}}')" \
  sha256:8592a8cefd33e94221cce3ba1b0681c1747fc1f6b694271983c2a4d3bd1043ee
require_equal redis_image_id \
  "$(docker image inspect "$redis_image" --format '{{.Id}}')" \
  sha256:2a51817f79255c8b69f86a974459c2e0359aff81417d80158f2b9e541e6f4b33

source_checksums=$(docker run --rm --entrypoint /bin/bash "$source_image" -lc \
  'sha256sum /app/obbinlog-ce.rpm /app/config.sh /app/start_services.sh')
grep -q '^ecab5d0a83d6d28d1d61783557f8b5529af02ba6fcf8a8700cf0a02f20022b74  /app/obbinlog-ce.rpm$' \
  <<<"$source_checksums"
grep -q '^39d0e79f23aa71d793bc76afcfab3506d4e06ed973303593943acfed3bc628ca  /app/config.sh$' \
  <<<"$source_checksums"
grep -q '^7160cb2265e7599ae213f95545d25b44d2dd776d14b3597b326bd0fe1edef6f2  /app/start_services.sh$' \
  <<<"$source_checksums"
printf '%s\n' "$source_checksums"

capability_source_checksums=$(docker run --rm --entrypoint /bin/bash \
  "$capability_source_image" -lc '
  sha256sum /app/initialize-before-binlog.sh
  grep -c "^/app/initialize-before-binlog.sh$" /app/start_services.sh
')
grep -q '^9d1583ad5858e773150e86986aeaffcefbe935e5dd898950fd19d8251e8a3493  /app/initialize-before-binlog.sh$' \
  <<<"$capability_source_checksums"
grep -q '^1$' <<<"$capability_source_checksums"
printf '%s\n' "$capability_source_checksums"

connector_checksums=$(docker run --rm --entrypoint /bin/bash "$debezium_image" -lc '
  sha256sum \
    /debezium/lib/debezium-connector-mysql-3.6.1.Final.jar \
    /debezium/lib/mysql-binlog-connector-java-0.41.0.jar \
    /debezium/lib/mysql-connector-j-9.6.0.jar \
    /debezium/lib/debezium-server-redis-3.6.1.Final.jar
')
for expected in \
  db56640086983fa838aaedffd7a2fc9887932fc028dbbfef11f62fc49248d5a2 \
  a4bd7e9e24269fdfbbdfcd692f100878dc2539b5f331276c7700fdde44341364 \
  66df1d453789dc8cb759a7dc17f58646893bf28483f262328650f170472a6ead \
  4dc44fd87e4cb80aef3b0066af9bd78623e8c83ea7de0bc8123bb8c413f06786; do
  grep -q "^$expected  " <<<"$connector_checksums"
done
printf '%s\n' "$connector_checksums"

connector_j=$(mktemp /tmp/oceanbase-client-2.4.18.XXXXXX.jar)
trap 'rm -f "$connector_j"' EXIT
curl -fsSL \
  https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.18/oceanbase-client-2.4.18.jar \
  -o "$connector_j"
require_equal oceanbase_connector_j_bytes "$(stat -c '%s' "$connector_j")" 1174658
require_equal oceanbase_connector_j_sha256 \
  "$(sha256sum "$connector_j" | awk '{print $1}')" \
  783b1f07de6205548d8caae62dea4d51156fffe6056f8bae172b3475cccf6864

manifest_digest=$(docker manifest inspect --verbose \
  oceanbase/oceanbase-ce:4.4.2.1-101000022026050611 \
  | jq -r '.[] | select(.Descriptor.platform.architecture == "amd64") | .Descriptor.digest')
require_equal oceanbase_candidate_image_amd64_manifest_digest "$manifest_digest" \
  sha256:46eefcf1275beae76947aac8f96c47efb745f6902d584de3c26465e4a9f113ae
