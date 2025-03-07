{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','version','event_index'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::DATE"],    
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash,event_type,event_address,event_module,event_resource,payload_function);",
    tags = ['core']
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    success,
    tx_type,
    payload_function,
    event_index,
    event_type,
    event_address,
    event_module,
    event_resource,
    event_data,
    account_address,
    creation_number,
    sequence_number,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','version','event_index']) }} AS fact_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref(
        'silver__events'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
