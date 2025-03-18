{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true, "columns": true },
    tags = ['noncore']
) }}

SELECT
    p.asset_id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    p.provider,
    p.source,
    p._inserted_timestamp,
    p.inserted_timestamp,
    p.modified_timestamp,
    p.complete_provider_prices_id,
    p._invocation_id
FROM
    {{ ref(
        'bronze__complete_provider_prices'
    ) }}
    p
    INNER JOIN {{ ref('bronze__complete_provider_asset_metadata') }}
    m
    ON p.asset_id = m.asset_id

{% if is_incremental() %}
WHERE
    p.modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY p.asset_id, recorded_hour, p.provider
ORDER BY
    p.modified_timestamp DESC)) = 1
