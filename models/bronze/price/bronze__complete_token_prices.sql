{{ config (
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    HOUR,
    'movement' AS token_address,
    asset_id,
    symbol,
    'Movement' AS NAME,
    decimals,
    price,
    'movement' AS blockchain,
    'movement' AS blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_prices'
    ) }}
WHERE
    asset_id = 'movement' qualify(ROW_NUMBER() over(PARTITION BY asset_id
ORDER BY
    is_deprecated, blockchain_id) = 1)
