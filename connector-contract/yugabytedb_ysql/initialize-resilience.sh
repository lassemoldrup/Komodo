#!/usr/bin/env bash
set -euo pipefail

bin/yb-admin --master_addresses yb_node_1:7100 \
  modify_placement_info \
  local.region.zone1,local.region.zone2,local.region.zone3 3

deadline=$((SECONDS + 180))
while ((SECONDS < deadline)); do
  live=$(bin/yb-admin --master_addresses yb_node_1:7100 list_all_tablet_servers 2>/dev/null \
    | awk 'NR > 1 && $0 !~ /DEAD/ { count += 1 } END { print count + 0 }')
  ((live >= 3)) && break
  sleep 1
done
((live >= 3))

exec env CONNECTOR_CONTRACT_DATABASE_HOST=yb_node_1 \
  /bin/bash /connector-contract/initialize.sh
