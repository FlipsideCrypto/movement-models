{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } },
    tags = ['noncore']
) }}

SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    MIN(block_number) :: FLOAT AS block_number_min,
    MAX(block_number) :: FLOAT AS block_number_max,
    COUNT(
        DISTINCT block_number
    ) AS block_count,
    COUNT(
        DISTINCT tx_hash
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN success THEN tx_hash
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT success THEN tx_hash
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT sender
    ) AS unique_sender_count,
    COUNT(
        DISTINCT payload_function
    ) AS unique_payload_function_count,
    SUM(COALESCE(gas_unit_price,0) * gas_used) AS total_fees,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions') }}
WHERE
    block_timestamp_hour < DATE_TRUNC(
        'hour',
        CURRENT_TIMESTAMP
    )
GROUP BY
    1