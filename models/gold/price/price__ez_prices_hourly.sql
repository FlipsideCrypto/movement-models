{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'ez_prices_hourly_id',
    cluster_by = ['HOUR::DATE','is_native'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(token_address, symbol, NAME)",
    tags = ['noncore']
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    FALSE AS is_native,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_token_prices') }}
UNION ALL
SELECT
    HOUR,
    NULL AS token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    TRUE AS is_native,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_native_prices') }}
