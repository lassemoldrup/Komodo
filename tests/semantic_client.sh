#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=scripts/blackbox_lifecycle.sh
. "$ROOT/scripts/blackbox_lifecycle.sh"


load_rows() {
  blackbox_database_query "
    START TRANSACTION;
    INSERT INTO usertable
      (ycsb_key, field1, field2, field3, field4, field5,
       field6, field7, field8, field9, field10)
    VALUES
      (1, 'old', 'stable', NULL, '', 'é', '中', '0', 'hello', '🙂', 'last'),
      (2, 'old', 'stable', NULL, '', 'é', '中', '0', 'hello', '🙂', 'last');
    COMMIT;
  " >/dev/null
}


run_probe() {
  : "${BLACKBOX_BENCHBASE_BUILD_DIR:?set BLACKBOX_BENCHBASE_BUILD_DIR}"
  : "${BLACKBOX_BENCHBASE_ROOT:?set BLACKBOX_BENCHBASE_ROOT}"
  : "${BLACKBOX_MAVEN:?set BLACKBOX_MAVEN}"
  local classes="$BLACKBOX_BENCHBASE_BUILD_DIR/classes"
  [[ -d $classes ]] || {
    echo "BenchBase classes are missing: $classes" >&2
    return 1
  }
  [[ -f $BLACKBOX_BENCHBASE_ROOT/pom.xml ]] || {
    echo "BenchBase root does not contain pom.xml: $BLACKBOX_BENCHBASE_ROOT" >&2
    return 1
  }

  local maven=$BLACKBOX_MAVEN
  if [[ $maven == */* && $maven != /* ]]; then
    maven="$BLACKBOX_BENCHBASE_ROOT/$maven"
  fi
  if [[ $maven == */* ]]; then
    [[ -x $maven ]] || {
      echo "Maven is not executable: $maven" >&2
      return 1
    }
  elif ! command -v "$maven" >/dev/null 2>&1; then
    echo "Maven command is missing: $maven" >&2
    return 1
  fi

  cd "$BLACKBOX_BENCHBASE_ROOT"
  exec "$maven" --batch-mode --quiet --activate-profiles mariadb \
    -Dmaven.gitcommitid.skip=true \
    -DbuildDirectory="$BLACKBOX_BENCHBASE_BUILD_DIR" \
    -Dexec.executable=java \
    -Dexec.classpathScope=runtime \
    "-Dexec.args=-classpath %classpath com.oltpbenchmark.benchmarks.ycsb.FracturedReadProbe" \
    org.codehaus.mojo:exec-maven-plugin:3.4.1:exec
}


case "${1:-}" in
  load) load_rows ;;
  run) run_probe ;;
  *)
    echo "usage: $0 load|run" >&2
    exit 2
    ;;
esac
