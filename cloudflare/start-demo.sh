#!/usr/bin/env bash
set -euo pipefail

mkdir -p /app/logs /tmp/streamforge-pg /tmp/streamforge-kafka

log() {
  printf '[streamforge-runtime] %s\n' "$*"
}

dump_logs() {
  log "startup failed; dumping recent component logs"
  for f in /tmp/streamforge-postgres.log /app/logs/kafka.log /app/logs/downstream.log /app/logs/ingestion.log /app/logs/processor.log /app/logs/control-plane.log; do
    if [[ -f "$f" ]]; then
      printf '\n===== %s =====\n' "$f"
      tail -n 120 "$f" || true
    fi
  done
}

trap dump_logs ERR

wait_port() {
  local host="$1"
  local port="$2"
  local name="$3"
  local retries="${4:-90}"

  for ((i=1; i<=retries; i++)); do
    if nc -z "$host" "$port" >/dev/null 2>&1; then
      log "$name ready on $host:$port"
      return 0
    fi
    sleep 1
  done

  log "ERROR: $name did not become ready"
  return 1
}

# ---------------------------
# PostgreSQL (ephemeral)
# ---------------------------
PG_BIN="$(dirname "$(find /usr/lib/postgresql -type f -name initdb | sort | tail -n 1)")"
if [[ -z "$PG_BIN" ]]; then
  log "ERROR: PostgreSQL binaries not found"
  exit 1
fi

if [[ ! -f /tmp/streamforge-pg/PG_VERSION ]]; then
  rm -rf /tmp/streamforge-pg
  install -d -o postgres -g postgres /tmp/streamforge-pg
  su -s /bin/bash postgres -c "$PG_BIN/initdb -D /tmp/streamforge-pg --auth=trust --no-locale --encoding=UTF8" >/app/logs/postgres-init.log
fi

# pg_ctl runs as the postgres user, so its runtime and log targets must be writable by postgres.
install -d -o postgres -g postgres /var/run/postgresql
rm -f /tmp/streamforge-postgres.log
touch /tmp/streamforge-postgres.log
chown postgres:postgres /tmp/streamforge-postgres.log
su -s /bin/bash postgres -c "$PG_BIN/pg_ctl -D /tmp/streamforge-pg -o \"-h 127.0.0.1 -p 5432 -k /tmp\" -l /tmp/streamforge-postgres.log start"
wait_port 127.0.0.1 5432 postgres

if ! "$PG_BIN/psql" -h 127.0.0.1 -U postgres -Atqc "select 1 from pg_database where datname='streamforge'" | grep -q 1; then
  "$PG_BIN/createdb" -h 127.0.0.1 -U postgres streamforge
fi

# ---------------------------
# Kafka KRaft (ephemeral)
# ---------------------------
cat >/app/kafka.properties <<'EOF'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/tmp/streamforge-kafka
num.partitions=3
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
EOF

if [[ ! -f /tmp/streamforge-kafka/meta.properties ]]; then
  KAFKA_CLUSTER_ID="$(/opt/kafka/bin/kafka-storage.sh random-uuid)"
  /opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t "$KAFKA_CLUSTER_ID" -c /app/kafka.properties >/app/logs/kafka-format.log
fi

export KAFKA_HEAP_OPTS="-Xms128m -Xmx384m"
/opt/kafka/bin/kafka-server-start.sh /app/kafka.properties >/app/logs/kafka.log 2>&1 &
KAFKA_PID=$!
wait_port 127.0.0.1 9092 kafka 120

/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists \
  --topic streamforge.events --partitions 3 --replication-factor 1 >/dev/null
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists \
  --topic streamforge.dlq --partitions 3 --replication-factor 1 >/dev/null

# ---------------------------
# Spring Boot services
# ---------------------------
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
export DB_URL=jdbc:postgresql://127.0.0.1:5432/streamforge
export DB_USER=postgres
export DB_PASSWORD=streamforge-demo
export DOWNSTREAM_URL=http://127.0.0.1:8083
export INGESTION_URL=http://127.0.0.1:8081
export PROCESSOR_URL=http://127.0.0.1:8082

JAVA_LIMITS="-Xms32m -Xmx192m -XX:+UseSerialGC"

log "starting downstream simulator"
java $JAVA_LIMITS -jar /app/downstream-simulator.jar >/app/logs/downstream.log 2>&1 &
DOWNSTREAM_PID=$!
wait_port 127.0.0.1 8083 downstream 120

log "starting ingestion service"
java $JAVA_LIMITS -jar /app/ingestion-service.jar >/app/logs/ingestion.log 2>&1 &
INGESTION_PID=$!
wait_port 127.0.0.1 8081 ingestion 120

log "starting processor service"
java $JAVA_LIMITS -jar /app/processor-service.jar >/app/logs/processor.log 2>&1 &
PROCESSOR_PID=$!
wait_port 127.0.0.1 8082 processor 120

log "starting control plane"
java $JAVA_LIMITS -jar /app/control-plane-service.jar >/app/logs/control-plane.log 2>&1 &
CONTROL_PID=$!
wait_port 127.0.0.1 8080 control-plane 120

cleanup() {
  set +e
  log "stopping StreamForge runtime"
  kill "$CONTROL_PID" "$PROCESSOR_PID" "$INGESTION_PID" "$DOWNSTREAM_PID" "$KAFKA_PID" 2>/dev/null || true
  su -s /bin/bash postgres -c "$PG_BIN/pg_ctl -D /tmp/streamforge-pg stop -m fast" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM
trap - ERR

log "StreamForge ready: http://127.0.0.1:8080"
wait "$CONTROL_PID"
