{{ config(
    materialized = 'incremental',
    unique_key = 'version',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, account_address,token_address);",
    tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    event_index,
    creation_number,
    transfer_event,
    account_address,
    amount,
    token_address,
    FALSE AS is_fungible,
    NULL :: STRING AS store_address,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index','version']
    ) }} AS fact_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'silver__transfers'
    ) }}
WHERE
    amount <> 0

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
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    event_index,
    NULL AS creation_number,
    transfer_event,
    owner_address AS account_address,
    amount,
    metadata_address AS token_address,
    TRUE AS is_fungible,
    store_address,
    transfers_fungible_id AS fact_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
FROM
    {{ ref('silver__transfers_fungible') }}
WHERE
    amount <> 0

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
