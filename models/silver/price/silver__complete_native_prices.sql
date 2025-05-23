{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true, "columns": true },
    tags = ['noncore']
) }}

SELECT
    HOUR,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id,
    _invocation_id
FROM
    {{ ref(
        'bronze__complete_native_prices'
    ) }}
