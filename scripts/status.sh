#!/usr/bin/env bash
# Health overview.
set -uo pipefail

echo "=== Containers ==="
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}'

echo
echo "=== Binlog position ==="
docker exec cdc-mariadb mariadb --skip-ssl -uroot -p"${MARIADB_ROOT_PASSWORD:-rootpw}" \
  -e "SHOW BINLOG STATUS\G SELECT @@gtid_binlog_pos AS gtid_binlog_pos;" 2>/dev/null

echo "=== Debezium ==="
if docker logs cdc-debezium 2>&1 | grep -q 'Starting streaming'; then
  echo "streaming: yes"
else
  echo "streaming: NO"
fi
docker logs cdc-debezium 2>&1 | grep -cE '\bERROR\b' | xargs -I{} echo "error lines in log: {}"

echo
echo "=== Redis stream ==="
echo "entries: $(docker exec cdc-redis redis-cli XLEN cdc 2>/dev/null)"
docker exec cdc-redis redis-cli INFO memory 2>/dev/null | grep -E '^used_memory_human' | tr -d '\r'
