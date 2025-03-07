{{ config(
    materialized = 'incremental',
    unique_key = ['fact_blocks_id'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::DATE"],    
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    first_version,
    last_version,
    tx_count_from_versions AS tx_count,
    {{ dbt_utils.generate_surrogate_key(['block_number']) }} AS fact_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id

FROM
    {{ ref(
        'silver__blocks'
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
