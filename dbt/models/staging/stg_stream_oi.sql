SELECT
    minute,
    symbol,
    open_interest,
    open_interest_value,
    open_interest_change_5m,
    open_interest_change_15m,
    open_interest_change_1h,
    (open_interest < 0 OR open_interest_value < 0) AS has_negative_open_interest
FROM {{ source('stream', 'oi') }}
