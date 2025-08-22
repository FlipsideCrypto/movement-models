{{ config(
    materialized = 'incremental',
    unique_key = "nft_mints_combined_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, nft_from_address, nft_to_address, nft_address);",
    tags = ['noncore']
) }}

{% if execute %}

{% if is_incremental() %}
{% set min_bts_query %}

SELECT
    MIN(block_timestamp) :: DATE
FROM
    {{ ref('silver__nft_mints_v2') }}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_bts = run_query(min_bts_query) [0] [0] %}
    {% if not min_bts or min_bts == 'None' %}
        {% set min_bts = '2099-01-01' %}
    {% endif %}
{% endif %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_mints_v1') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    *
FROM
    {{ ref('silver__nft_mints_v2') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
xfers AS (
    SELECT
        tx_hash,
        event_index,
        token_address
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        success

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    tx_hash,
    event_index,
    metadata_address AS token_address
FROM
    {{ ref('silver__transfers_fungible') }}
WHERE
    success

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{ min_bts }}'
{% endif %}
)
SELECT
    mints.block_timestamp,
    mints.block_number,
    mints.version,
    mints.tx_hash,
    mints.event_index,
    mints.event_type,
    mints.nft_address,
    mints.project_name,
    mints.nft_from_address,
    mints.nft_to_address,
    mints.tokenid,
    mints.token_version,
    mints.nft_count,
    mints.price_raw AS total_price_raw,
    transfers.token_address AS currency_address,
    {{ dbt_utils.generate_surrogate_key(
        ['mints.tx_hash','mints.event_index']
    ) }} AS nft_mints_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base mints
    JOIN xfers transfers
    ON mints.tx_hash = transfers.tx_hash qualify ROW_NUMBER() over (
        PARTITION BY mints.tx_hash,
        mints.event_index
        ORDER BY
            transfers.event_index DESC
    ) = 1