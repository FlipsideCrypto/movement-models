{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    incremental_strategy = 'delete+insert',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::DATE"],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash,event_type,event_address,event_module,event_resource,payload_function);",
    tags = ['core']
) }}

SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    version,
    success,
    A.tx_type,
    A.sender,
    A.payload_function,
    b.index AS event_index,
    b.value :type :: STRING AS event_type,
    SPLIT_PART(
        event_type,
        '::',
        1
    ) :: STRING AS event_address,
    SPLIT_PART(
        event_type,
        '::',
        2
    ) :: STRING AS event_module,
    SPLIT_PART(
        event_type,
        '::',
        3
    ) :: STRING AS event_resource,
    b.value :data AS event_data,
    -- b.value :guid :: STRING AS event_guid, -- extract into account_address + creation_number
    b.value :guid :account_address :: STRING AS account_address,
    b.value :guid :creation_number :: bigint AS creation_number,
    b.value :sequence_number :: bigint AS sequence_number,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','version','event_index']) }} AS fact_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'core__fact_transactions'
    ) }} A,
    LATERAL FLATTEN (events) b

{% if is_incremental() %}
WHERE
    A.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
