#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
expected_base=sha256:26eef9c42eeab6f49ae9035aa9cb75317f42839e03ef46ee667293fbf7336661
base_image=db-monitoring/oceanbase-obbinlog-contract:4.4.2.1-obbinlog4.3.6.4
source_image=db-monitoring/oceanbase-mysql-capability-source:4.4.2.1-obbinlog4.3.6.4

actual_base=$(docker image inspect "$base_image" --format '{{.Id}}')
[[ $actual_base == "$expected_base" ]]

docker build --file "$root/Dockerfile.source" --tag "$source_image" "$root"
docker image inspect "$source_image" --format '{{.Id}}'
