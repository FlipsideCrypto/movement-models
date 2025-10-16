{{ config(
    materialized = 'incremental',
    unique_key = 'version',
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash);",
    tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    tx_type,
    vm_status,
    state_checkpoint_hash,
    accumulator_root_hash,
    event_root_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS fact_transactions_state_checkpoint_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('core__fact_transactions') }}
WHERE
    LEFT(
        tx_type,
        5
    ) = 'state'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
