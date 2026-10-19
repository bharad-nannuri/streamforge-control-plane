# StreamForge Control Plane

**A distributed workload orchestration, resilience, and observability platform built to make production failure behavior visible.**

StreamForge is a hands-on systems project centered on the problems that matter after a service leaves a developer laptop: event delivery, retries, dead-letter handling, replay, idempotency, dependency failures, application-level metrics, and recovery.

## What the live demo is designed to show

1. Generate traffic through a Spring Boot ingestion API.
2. Publish events to Kafka and process them asynchronously.
3. Persist event state and idempotency keys in PostgreSQL.
4. Inject downstream latency, HTTP 500s, or unavailability.
5. Watch retries and DLQ depth grow in the control-plane dashboard.
6. Restore the dependency and replay failed events.
7. Verify backlog recovery without duplicate processing.

## Technology

- Java 21 / Spring Boot 3
- Apache Kafka
- PostgreSQL
- Micrometer / Prometheus
- Docker / Docker Compose
- GitHub Actions
- Self-contained HTML/CSS/JavaScript control-plane UI

## Run locally

```bash
docker compose up --build
```

Open http://localhost:8080.

## Demo flow

1. Generate 100 Events
2. Inject HTTP 500 or latency
3. Generate additional workload and watch lag/retries/DLQ
4. Restore Healthy
5. Replay DLQ
6. Watch the backlog drain

See [docs/architecture.md](docs/architecture.md) and [docs/design-decisions.md](docs/design-decisions.md).

## Public demo note

The portfolio deployment is intentionally a demo environment and may use ephemeral runtime state. The reference architecture keeps service boundaries explicit so components can be deployed independently in a production environment.
