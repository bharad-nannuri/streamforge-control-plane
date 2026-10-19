# Architecture

## Goal

Demonstrate how a distributed event-processing system behaves under healthy load, partial failure, retry pressure, dead-letter handling, and recovery.

```text
Browser -> Control Plane -> Ingestion API -> Kafka -> Processor -> Downstream Simulator
                                              |           |
                                              |           +-> PostgreSQL event state
                                              |
                                              +-> retry -> DLQ -> replay -> Kafka
```

### ingestion-service
Accepts events over HTTP, applies idempotency keys, persists the event envelope, and publishes it to Kafka.

### processor-service
Consumes Kafka records, tracks attempts, calls the downstream dependency, marks successful events, and routes exhausted failures to the DLQ.

### downstream-simulator
Provides HEALTHY, LATENCY, CHAOS, FAIL_500, and UNAVAILABLE modes.

### control-plane-service
Serves the dashboard, generates traffic, changes failure modes, exposes aggregate event metrics, reports dependency health, measures Kafka consumer lag, and replays DLQ events.

## Delivery semantics

The runtime intentionally uses **at-least-once delivery**. Event IDs and idempotency keys are persisted so duplicate work can be detected and skipped.
