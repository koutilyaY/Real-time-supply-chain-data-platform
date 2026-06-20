-- Dedup to one row per (carrier, window_start); keep the most-complete emission.
with src as (
    select
        carrier,
        window_start,
        window_end,
        total_shipments,
        delayed_shipments,
        delay_rate,
        row_number() over (
            partition by carrier, window_start
            order by total_shipments desc
        ) as _rn
    from {{ source('silver', 'shipment_delays_5m') }}
)
select carrier, window_start, window_end, total_shipments, delayed_shipments, delay_rate
from src
where _rn = 1
