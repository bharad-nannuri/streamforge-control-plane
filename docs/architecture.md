# StreamForge v1 Architecture

## Goal

Demonstrate how a distributed event-processing platform behaves under load and partial failure.

## Core flow

```text
Client
  |
  v
Ingestion Service
  |
  v
Kafka topic: streamforge.events
  |
  v
Processor Service
  |
  +----> PostgreSQL
  |
  +----> Downstream Simulator
            |
            +----> success
            +----> latency
            +----> HTTP 500

Failures
  |
  +----> retry topic(s)
  |
  +----> DLQ
          |
          v
        replay
```

## Design principles

- At-least-once event delivery.
- Idempotent processing to tolerate redelivery.
- Explicit retry and DLQ behavior.
- Application-level metrics, not only CPU/memory.
- Failure injection as a first-class demo capability.
- Recovery should be observable and explainable.
