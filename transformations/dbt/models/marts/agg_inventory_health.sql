-- Current inventory health by warehouse: how many SKUs below reorder point and
-- the total shortfall units. Uses the latest alert per (warehouse, sku).
with ranked as (
    select
        warehouse_id,
        sku,
        region,
        shortfall,
        row_number() over (
            partition by warehouse_id, sku order by detected_ts desc
        ) as rn
    from {{ ref('stg_inventory_alerts') }}
)
select
    warehouse_id,
    region,
    count(*)        as skus_below_reorder,
    sum(shortfall)  as total_shortfall_units
from ranked
where rn = 1
group by 1, 2
