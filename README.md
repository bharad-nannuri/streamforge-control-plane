# StreamForge Control Plane

> A distributed workload orchestration, resilience, and observability platform built to make failure behavior visible.

StreamForge is a portfolio-grade systems project focused on the engineering problems that matter in production: event delivery, backpressure, retries, dead-letter handling, replay, idempotency, service health, SLOs, failure injection, and recovery.

## Target architecture

```text
                       StreamForge UI
                             |
                             v
                     Control Plane API
                             |
          +------------------+------------------+
          |                  |                  |
          v                  v                  v
   Traffic Generator    Failure Controls    Replay Controls
          |                                     |
          v                                     |
      Ingestion API                              |
          |                                      |
          v                                      |
         Kafka  -------------------------------> Retry / DLQ
          |                                      |
          v                                      |
    Processing Worker ----------------------------+
          |
          +------------------> PostgreSQL
          |
          +------------------> Downstream Simulator

Observability: Micrometer + Prometheus + OpenTelemetry
Deployment: Docker Compose first; Kubernetes/Helm next
```

## v1 scope

- Java 21 / Spring Boot
- Kafka-compatible event broker
- PostgreSQL
- retries + DLQ + replay
- idempotency protection
- downstream failure simulation
- application-level metrics
- React operations dashboard
- Docker Compose local environment
- GitHub Actions CI

## What the demo will show

1. Generate traffic.
2. Watch throughput, latency, failures, retries, DLQ depth, and lag.
3. Inject latency or HTTP 500 failures.
4. Observe degradation and backlog growth.
5. Restore the dependency.
6. Replay failed events.
7. Verify the backlog drains without duplicate processing.

## Status

Initial implementation in progress.
