# Backend selection: DB=mariadb (default) | yugabyte | oracle
DB ?= mariadb
COMPOSE = docker compose --profile $(DB)

.PHONY: up down reset status stream follow txn bench sql logs e2e test-lifecycle test-lifecycle-live

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

## Run the audited MariaDB lifecycle (requires the three BLACKBOX_*_COMMAND hooks)
e2e:
	./scripts/e2e.sh

## Fast protocol/lifecycle tests with controlled monitor acknowledgements
test-lifecycle:
	python3 protocol/tools/verify_protocol.py
	shellcheck scripts/*.sh tests/*.sh
	bash -n scripts/*.sh tests/*.sh
	python3 -m unittest discover -s tests -p 'test_*.py' -v
	./tests/test_e2e_lifecycle.sh

## Destructive integration against the pinned MariaDB/Debezium/Redis stack
test-lifecycle-live:
	./tests/test_mariadb_lifecycle.sh
