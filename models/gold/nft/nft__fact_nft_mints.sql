{{ config(
    materialized = 'incremental',
    unique_key = ['fact_nft_mints_id'],
    incremental_strategy = 'merge',
    cluster_by = ['modified_timestamp::DATE'],
    merge_exclude_columns = ['inserted_timestamp'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(nft_address);",
    tags = ['noncore']
) }}

SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_type,
    nft_from_address,
    nft_to_address,
    nft_address,
    token_version,
    project_name,
    tokenid,
    nft_count,
    total_price_raw,
    currency_address,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS fact_nft_mints_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__nft_mints_combined') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}