{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','to_address','from_address','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, from_address, to_address);",
  tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    from_address,
    to_address,
    amount,
    token_address,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','to_address','from_address','block_timestamp::DATE']
    ) }} AS ez_native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'silver__transfers_native'
    ) }}

{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}