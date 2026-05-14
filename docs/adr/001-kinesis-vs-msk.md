# ADR 001: Kinesis vs MSK

## Decision

Use Kinesis Data Streams for the MVP stream spine, with contracts and consumers portable enough to move to MSK/Kafka if the workload proves it needs Kafka-native operations.

## Context

The team has a small senior-engineer allocation, an AWS constraint, and a short MVP window. The pipeline needs replayable streaming, multiple consumers, per-tenant controls, and operational simplicity.

## Options

| Option | Strength | Weakness |
|---|---|---|
| Kinesis Data Streams | AWS-native, managed, shard-based capacity, simpler ops. | Hot-key caveats, AWS-specific APIs, shard and quota planning. |
| MSK / Kafka | Kafka ecosystem, portable client semantics, mature connector model. | More operational load, broker/storage/partition planning. |
| MSK Serverless | Kafka API with less broker management. | Quota and scaling shape still must be verified for this workload. |

## Rationale

Kinesis-first fits the MVP because AWS runs the stream service and the team can spend its limited time on ingestion semantics, reconciliation, tenant isolation, and Flink jobs. For known launches, I would not rely on vague on-demand magic. I would provision or warm capacity, check Service Quotas, and rehearse fallback before the event.

## When To Switch

Switch to MSK if replay workflows require richer Kafka tooling, tenant isolation is cleaner with topics/partitions, exporter ecosystem needs Kafka Connect, cost at scale favors Kafka, or Kinesis hot-key behavior becomes the dominant risk.
