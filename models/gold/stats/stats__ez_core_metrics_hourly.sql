{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } },
    tags = ['noncore']
) }}

SELECT
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_sender_count,
    unique_payload_function_count,
    total_fees AS total_fees_native,
    ROUND(
        (total_fees / pow(
            10,
            8
        )) * p.price,
        2
    ) AS total_fees_usd,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    s.inserted_timestamp AS inserted_timestamp,
    s.modified_timestamp AS modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}
    s
    JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.is_native
WHERE
    block_timestamp_hour < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
{% if is_incremental() %}
    AND block_timestamp_hour >= DATEADD('hour', -{{ var('HOURLY_METRICS_LOOKBACK_HOURS', 24) }}, (
        SELECT 
            DATE_TRUNC('hour', MAX(modified_timestamp))
        FROM {{ this }}
    ))
{% endif %}