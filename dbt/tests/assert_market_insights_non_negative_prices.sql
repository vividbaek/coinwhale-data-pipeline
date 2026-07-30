SELECT
    ts,
    symbol,
    mark_price,
    last_price,
    open_interest,
    'market_insights_5s' AS source_view
FROM {{ ref('market_insights_5s') }}
WHERE
    ts >= now() - INTERVAL 1 DAY
    AND (mark_price < 0 OR last_price < 0 OR open_interest < 0)

UNION ALL

SELECT
    ts,
    symbol,
    mark_price,
    last_price,
    open_interest,
    'market_insights_10s' AS source_view
FROM {{ ref('market_insights_10s') }}
WHERE
    ts >= now() - INTERVAL 1 DAY
    AND (mark_price < 0 OR last_price < 0 OR open_interest < 0)

LIMIT 100
