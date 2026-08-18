# Backend selection: DB=mariadb (default) | yugabyte | oracle
DB ?= mariadb
COMPOSE = docker compose --profile $(DB)
MONITOR_BUILD_DIR ?= /tmp/db-monitoring-blackbox-monitor-build
CO_CHECKER_ROOT ?= $(abspath ../co-checker)
BENCHBASE_ROOT ?= $(abspath ../benchbase)
PROBE_BUILD_DIR ?= /tmp/db-monitoring-fractured-read-probe-build
MAVEN ?= mvn

.PHONY: up down reset status stream follow txn bench sql logs e2e monitor-build probe-client-build test-monitor test-lifecycle test-lifecycle-live test-correctness-live

## Start the stack for $(DB)
up:
	$(COMPOSE) up -d --build
	@echo "waiting for Debezium to reach streaming ..."
	@if [ "$(DB)" = "yugabyte" ]; then C=cdc-debezium-yb; \
	 elif [ "$(DB)" = "oracle" ]; then C=cdc-debezium-oracle; \
	 else C=cdc-debezium; fi; \
	  until docker logs $$C 2>&1 | grep -q 'Starting streaming'; do sleep 2; done
	@echo "ready ($(DB)). stream: redis://localhost:6379 key 'cdc'"

## Stop the stack for $(DB)
down:
	$(COMPOSE) down

## Stop and wipe all state for $(DB)
reset:
	$(COMPOSE) down -v

## Health overview
status:
	@DB=$(DB) ./scripts/status.sh

## Dump the CDC stream so far
stream:
	@DB=$(DB) ./scripts/stream.sh

## Follow the CDC stream live
follow:
	@DB=$(DB) ./scripts/stream.sh -f

## Run one manual write transaction for raw CDC exploration
txn:
	@DB=$(DB) ./scripts/txn.sh

## Throughput + latency benchmark (MariaDB only for now)
bench:
	@DB=$(DB) ./scripts/bench.sh

## SQL shell
sql:
	@DB=$(DB) ./scripts/sql.sh

logs:
	$(COMPOSE) logs -f --tail=100

## Run the audited MariaDB lifecycle (requires loader/workload command hooks)
e2e: monitor-build
	BLACKBOX_MONITOR_BIN=$(MONITOR_BUILD_DIR)/blackbox_run_monitor \
		./scripts/e2e.sh

## Fast protocol/lifecycle tests with controlled monitor acknowledgements
monitor-build:
	cmake -S monitor -B $(MONITOR_BUILD_DIR) -G Ninja -DCMAKE_BUILD_TYPE=Debug \
		-DCO_CHECKER_ROOT=$(CO_CHECKER_ROOT)
	cmake --build $(MONITOR_BUILD_DIR)

probe-client-build:
	mkdir -p $(PROBE_BUILD_DIR)
	cd $(BENCHBASE_ROOT) && $(MAVEN) --batch-mode --activate-profiles mariadb \
		-Dmaven.gitcommitid.skip=true \
		-DbuildDirectory=$(PROBE_BUILD_DIR) \
		-DskipTests compile

test-monitor: monitor-build
	ctest --test-dir $(MONITOR_BUILD_DIR) --output-on-failure

test-lifecycle: test-monitor
	python3 protocol/tools/verify_protocol.py
	shellcheck scripts/*.sh tests/*.sh
	bash -n scripts/*.sh tests/*.sh
	python3 -m unittest discover -s tests -p 'test_*.py' -v
	./tests/test_e2e_lifecycle.sh

## Destructive integration against the pinned MariaDB/Debezium/Redis stack
test-lifecycle-live: test-monitor
	BLACKBOX_CDC_REPLAY_BIN=$(MONITOR_BUILD_DIR)/blackbox_cdc_replay \
	BLACKBOX_CDC_INGEST_BIN=$(MONITOR_BUILD_DIR)/blackbox_cdc_ingest \
	BLACKBOX_PREFIX_INGEST_BIN=$(MONITOR_BUILD_DIR)/blackbox_prefix_ingest \
	BLACKBOX_MONITOR_BIN=$(MONITOR_BUILD_DIR)/blackbox_run_monitor \
		./tests/test_mariadb_lifecycle.sh

## Destructive semantic acceptance runs against fresh MariaDB/CDC stacks
test-correctness-live: test-monitor probe-client-build
	BLACKBOX_MONITOR_BIN=$(MONITOR_BUILD_DIR)/blackbox_run_monitor \
	BLACKBOX_BENCHBASE_BUILD_DIR=$(PROBE_BUILD_DIR) \
		./tests/test_semantic_correctness.sh
