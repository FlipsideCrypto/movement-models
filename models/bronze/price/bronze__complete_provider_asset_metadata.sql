{{ config (
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    asset_id,
    'movement' AS token_address,
    NAME,
    symbol,
    'movement' AS platform,
    platform_id,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_provider_asset_metadata'
    ) }}
WHERE
    asset_id = 'movement'
    AND token_address IS NULL
