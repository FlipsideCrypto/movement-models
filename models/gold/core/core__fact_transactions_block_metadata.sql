{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','block_timestamp::DATE'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::DATE"],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash);",
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
    A.failed_proposer_indices,
    A.id,
    A.previous_block_votes_bitvec,
    A.proposer,
    A.round,
    A.vm_status,
    A.state_change_hash,
    A.accumulator_root_hash,
    A.event_root_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS fact_transactions_block_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__transactions'
    ) }}
WHERE
    LEFT(
        tx_type,
        5
    ) = 'block'

{% if is_incremental() %}
AND A.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
