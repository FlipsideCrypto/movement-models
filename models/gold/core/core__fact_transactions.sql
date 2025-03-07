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
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    tx_type,
    sender,
    signature,
    payload,
    payload_function,
    changes,
    events,
    gas_unit_price,
    gas_used,
    max_gas_amount,
    expiration_timestamp_secs,
    vm_status,
    state_change_hash,
    accumulator_root_hash,
    event_root_hash,
    state_checkpoint_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS fact_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
