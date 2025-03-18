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
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON s.block_timestamp_hour = p.hour
WHERE
    p.is_native
    {% if is_incremental() %}
        AND s.block_timestamp_hour >= (
            SELECT 
                MAX(block_timestamp_hour) 
            FROM {{ this }})
    {% endif %}