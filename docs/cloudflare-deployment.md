# Cloudflare deployment

The public portfolio demo is designed for **labs.bharadnannuri.com**.

## Why a single container?

The reference architecture keeps Kafka, PostgreSQL and Spring Boot services independently deployable. For the public portfolio environment, the Cloudflare Container intentionally co-locates them into one ephemeral instance:

```text
Cloudflare Worker
      |
      v
StreamForge Demo Container
  + Kafka (KRaft)
  + PostgreSQL
  + ingestion-service
  + processor-service
  + downstream-simulator
  + control-plane-service :8080
```

This minimizes cost and operational overhead for a low-traffic interactive demo. It is explicitly **not** presented as the production topology.

## State model

The demo filesystem and database are disposable. If the container sleeps or restarts, the environment may start empty. That is acceptable and useful for a portfolio lab: every visitor receives a clean playground and no user data is expected to persist.

## Cloudflare configuration

`wrangler.jsonc`:
- Worker name: `streamforge-labs`
- Custom domain: `labs.bharadnannuri.com`
- One `standard-1` Container instance
- 20 minute idle sleep window
- Workers observability enabled

## Deploy

After the account has Containers enabled:

```bash
npm install
npx wrangler deploy
```

The deployment builds `Dockerfile.cloudflare`, uploads the image, deploys the Worker and attaches the custom domain.

## Local reference environment

For normal development, use the multi-container topology:

```bash
docker compose up --build
```

That environment preserves the service boundaries that the public single-container packaging intentionally collapses.
