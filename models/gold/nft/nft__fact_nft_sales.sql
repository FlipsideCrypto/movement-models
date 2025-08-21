{{ config(
    materialized = 'incremental',
    unique_key = ['fact_nft_sales_id'],
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
    buyer_address,
    seller_address,
    nft_address,
    token_version,
    platform_address,
    project_name,
    platform_name,
    platform_exchange_version,
    total_price_raw,
    nft_sales_tradeport_id AS fact_nft_sales_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_tradeport') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}