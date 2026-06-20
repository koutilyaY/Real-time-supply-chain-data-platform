select
    warehouse_id,
    sku,
    on_hand,
    reorder_point,
    shortfall,
    region,
    detected_ts
from {{ source('silver', 'inventory_alerts') }}
