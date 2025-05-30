{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','event_index','version','block_timestamp::DATE'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, account_address,token_address);",
    tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    event_index,
    creation_number,
    transfer_event,
    account_address,
    amount,
    token_address,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index','version']
    ) }} AS fact_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__transfers'
    ) }}
WHERE
    amount <> 0

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
