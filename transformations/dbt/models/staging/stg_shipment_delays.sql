select
    carrier,
    window_start,
    window_end,
    total_shipments,
    delayed_shipments,
    delay_rate
from {{ source('silver', 'shipment_delays_5m') }}
