{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true, "columns": true },
    tags = ['noncore']
) }}

SELECT
    HOUR,
    LOWER(
        token_address
    ) AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    blockchain_name,
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
    {{ ref(
        'bronze__complete_token_prices'
    ) }}
