# ADR 002: ClickHouse Dashboard Store

## Decision

Use ClickHouse as the primary dashboard OLAP store, with Redis reserved for hot personalization state and S3 as the raw system of record for replay and reconciliation.

## Rationale

Product analytics dashboards are high-cardinality, filter-heavy, and aggregation-heavy. ClickHouse is a better fit than Redis or PostgreSQL for that shape. The model should stay append-friendly:

- raw event table with tenant and time in the sort key;
- coarse time partitioning, not tenant partitioning;
- incremental materialized views for repeated dashboard aggregates;
- batched inserts or async inserts with durable acknowledgement mode;
- controlled mutation/delete workflows instead of frequent hard deletes.

## GDPR / Delete Limitations

ClickHouse should not be sold as immediate hard-delete compliance for every derived and raw copy. Hot personalization state can be deleted quickly. ClickHouse deletes or masking may lag behind operationally. S3 raw stores require retention policy, crypto-shredding, delete manifests, or partition expiration approved by legal.

## Managed vs Self-Hosted

Managed ClickHouse is preferable for the small team if budget permits. Self-hosted remains a fallback if commercial or compliance constraints require it, but it adds operational ownership that should not be hidden in the MVP plan.

## Timestream

Timestream may be useful for narrow operational time-series metrics. It should not be the primary product-analytics dashboard store unless real query patterns prove it is enough.
