{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'fact_prices_ohlc_hourly_id',
    cluster_by = ['HOUR::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, provider)",
    tags = ['noncore']
) }}

SELECT
    asset_id,
    recorded_hour AS HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_prices_id AS fact_prices_ohlc_hourly_id
FROM
    {{ ref('silver__complete_provider_prices') }}
