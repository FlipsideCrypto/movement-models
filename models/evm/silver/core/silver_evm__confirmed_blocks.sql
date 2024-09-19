-- depends_on: {{ ref('bronze_evm__streamline_confirm_blocks') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "round(block_number,-3)",
    tags = ['core']
) }}

WITH base AS (

    SELECT
        VALUE :BLOCK_NUMBER :: INT AS block_number,
        DATA :result :hash :: STRING AS block_hash,
        DATA :result :transactions txs,
        inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_evm__streamline_confirm_blocks') }}
WHERE
    inserted_timestamp >= (
        SELECT
            IFNULL(
                MAX(
                    inserted_timestamp
                ),
                '1970-01-01' :: TIMESTAMP
            ) inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze_evm__streamline_FR_confirm_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    inserted_timestamp DESC)) = 1
)
SELECT
    block_number,
    block_hash,
    VALUE :: STRING AS tx_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS confirmed_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base,
    LATERAL FLATTEN (
        input => txs
    )
