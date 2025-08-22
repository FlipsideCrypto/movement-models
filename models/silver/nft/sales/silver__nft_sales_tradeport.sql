{{ config(
    materialized = 'incremental',
    unique_key = "nft_sales_tradeport_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH events AS (

    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        account_address,
        event_address,
        event_resource,
        event_data,
        event_module,
        event_type,
        CASE
            WHEN event_resource = 'BuyEvent' THEN 'sale'
            WHEN event_resource IN (
                'AcceptCollectionBidEvent',
                'AcceptTokenBidEvent'
            ) THEN 'bid_won'
        END AS event_kind,
        modified_timestamp
    FROM
        {{ ref('core__fact_events') }}
    WHERE
        event_address = '0xf81bea5757d1ff70b441b1ec64db62436df5f451cde6eab81aec489791f22aa0'
        AND event_resource IN (
            'BuyEvent',
            'AcceptCollectionBidEvent',
            'AcceptTokenBidEvent'
        )
        AND success

{% if is_incremental() %}
AND modified_timestamp >= GREATEST(
    (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ),
    SYSDATE() :: DATE - 3
)
{% endif %}
),
chngs AS (
    SELECT
        block_timestamp,
        tx_hash,
        change_data,
        address,
        inner_change_type,
        change_resource
    FROM
        {{ ref('core__fact_changes') }}
    WHERE
        success
        AND inner_change_type = '0x4::collection::Collection'

{% if is_incremental() %}
AND modified_timestamp >= GREATEST(
    (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ),
    SYSDATE() :: DATE - 3
)
{% endif %}
),
nft_collection_lookup AS (
    SELECT DISTINCT
        nft_address,
        project_name
    FROM
        {{ ref('silver__nft_mints_combined') }}
    WHERE
        project_name IS NOT NULL
),
sales_with_nft_info AS (
    SELECT
        *,
        event_data :token :inner :: STRING AS nft_address
    FROM
        events
)
SELECT
    main.block_timestamp,
    main.block_number,
    main.version,
    main.tx_hash,
    main.event_index,
    main.event_kind AS event_type,
    COALESCE(main.event_data :buyer, main.event_data :bid_buyer) :: STRING AS buyer_address,
    COALESCE(main.event_data :seller, main.event_data :bid_seller) :: STRING AS seller_address,
    main.nft_address,
    lookup.project_name,
    'v2' AS token_version,
    main.event_address AS platform_address,
    'Tradeport' AS platform_name,
    'tradeport_marketplace_token_v1' AS platform_exchange_version,
    main.event_data :price :: NUMBER AS total_price_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['main.tx_hash','main.event_index']
    ) }} AS nft_sales_tradeport_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    sales_with_nft_info main
    LEFT JOIN nft_collection_lookup lookup
    ON main.nft_address = lookup.nft_address 