# Supplier Performance & Reorder Policy

## On-Time-In-Full (OTIF)
A supplier is considered compliant when its OTIF rate is at or above 90% over a
rolling 90-day window. Suppliers below 90% are placed on a watch list; below 80%
triggers a sourcing review.

## Reorder Logic
- Reorder point = average daily demand × lead-time days × safety factor (1.2).
- When on-hand inventory for a SKU drops below its reorder point, a replenishment
  order is raised automatically to the primary supplier.
- If the primary supplier's risk score exceeds 60, route the order to the backup
  supplier and notify the procurement owner.

## Lead Time
Standard domestic lead time is 5–10 days; international is 20–45 days depending on
customs and carrier. Lead-time variance above 30% should be flagged for review.
