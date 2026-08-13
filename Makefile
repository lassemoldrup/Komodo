.PHONY: up down reset status stream follow txn bench sql logs

## Start the stack
up:
	docker compose up -d
	@echo "waiting for Debezium to reach streaming ..."
	@until docker logs cdc-debezium 2>&1 | grep -q 'Starting streaming'; do sleep 1; done
	@echo "ready. stream: redis://localhost:6379 key 'cdc'"

## Stop everything
down:
	docker compose down

## Stop and wipe all state (DB is tmpfs, so this also clears the schema)
reset:
	docker compose down -v

## Health overview
status:
	./scripts/status.sh

## Dump the CDC stream so far
stream:
	./scripts/stream.sh

## Follow the CDC stream live
follow:
	./scripts/stream.sh -f

## Run one write transaction and print its GTID
txn:
	./scripts/txn.sh

## Throughput + latency benchmark
bench:
	./scripts/bench.sh

## SQL shell
sql:
	./scripts/sql.sh

logs:
	docker compose logs -f --tail=100
