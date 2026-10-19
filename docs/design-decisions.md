# Design Decisions

## At-least-once delivery
At-least-once delivery is operationally practical for Kafka-style systems, but requires idempotent processing. StreamForge makes that tradeoff visible rather than pretending duplicates cannot happen.

## Idempotency
The ingestion API accepts an optional `idempotencyKey`. A repeated key returns the original event. The processor also skips external work for an event already marked PROCESSED.

## Retry policy
The processor retries a failed record twice with fixed backoff. After retries are exhausted the original record is published to `streamforge.dlq`. A production policy would normally be dependency-specific and may use exponential backoff with jitter.

## DLQ and replay
A DLQ listener marks failed events as DLQ. The control plane can republish them to the main topic while preserving the event ID.

## Observability
StreamForge exports application-specific metrics for accepted, processed, failed, duplicate, DLQ and replayed events, plus processing duration. The dashboard also calculates P95 latency and reads Kafka consumer lag through the Admin API.

## Failure injection
The downstream simulator can inject latency, intermittent chaos, HTTP 500, and HTTP 503 behavior. Failure is a first-class product capability because resilient systems should be testable under degradation.
