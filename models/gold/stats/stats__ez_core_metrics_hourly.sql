{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } },
    tags = ['noncore']
) }}

-- depends_on: {{ ref('core__fact_transactions') }}

{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    DATE_TRUNC('hour', MIN(block_timestamp)) AS block_timestamp_hour 
        FROM
            {{ ref('core__fact_transactions') }}
        WHERE
            modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
         {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}

{% if not min_block_timestamp_hour or min_block_timestamp_hour == 'None' %}
    {% set min_block_timestamp_hour = '2099-01-01' %}
{% endif %}
{% endif %}

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
    AND p.is_native
WHERE
    block_timestamp_hour < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
{% if is_incremental() %}
AND
    block_timestamp_hour >= COALESCE(
        DATEADD('hour', -4, '{{ min_block_timestamp_hour }}'),
        '2025-01-01 00:00:00'
    )
{% endif %}