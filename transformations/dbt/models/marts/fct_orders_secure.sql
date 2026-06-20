-- Order-level facts with PII masked per governance/policies/masking.yml.
-- customer_id is hashed (sha256 + env salt) via the mask_hash macro, so analysts
-- can join/segment on a stable pseudonymous key without seeing the raw identifier.
select
    order_id,
    {{ mask_hash('customer_id') }} as customer_id_hash,
    sku,
    quantity,
    unit_price,
    currency,
    region,
    order_ts
from {{ source('bronze', 'orders_raw') }}
where order_id is not null
