{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','block_timestamp::DATE'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash,payload_function,sender);",
    tags = ['core']
) }}

SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    A.success,
    A.tx_type,
    A.sender,
    A.signature,
    A.payload,
    A.payload_function,
    A.changes,
    A.events,
    A.gas_unit_price,
    A.gas_used,
    A.max_gas_amount,
    A.expiration_timestamp_secs,
    A.vm_status,
    A.state_change_hash,
    A.accumulator_root_hash,
    A.event_root_hash,
    A.state_checkpoint_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS fact_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }} A

{% if is_incremental() %}
WHERE
    A.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
