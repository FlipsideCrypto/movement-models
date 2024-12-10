{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['noncore']
) }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('core__fact_transactions') }}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}
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
    SUM(gas_used) AS total_fees,
    -- in Octa = 10^-8 Aptos
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core__fact_transactions') }}
WHERE
    block_timestamp_hour < DATE_TRUNC(
        'hour',
        CURRENT_TIMESTAMP
    )

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= NULLIF(
    '{{ min_block_timestamp_hour }}',
    'None'
)
{% endif %}
GROUP BY
    1
