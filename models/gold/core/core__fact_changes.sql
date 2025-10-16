{{ config(
    materialized = 'incremental',
    unique_key = 'version',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(version,tx_hash,change_type,inner_change_type,change_address,change_module,change_resource,payload_function);",
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
    b.index AS change_index,
    b.value :data :data AS change_data,
    b.value :type :: STRING AS change_type,
    b.value :address :: STRING AS address,
    b.value :handle :: STRING AS handle,
    b.value :data: "type" :: STRING AS inner_change_type,
    SPLIT_PART(
        inner_change_type,
        '::',
        1
    ) :: STRING AS change_address,
    SPLIT_PART(
        inner_change_type,
        '::',
        2
    ) :: STRING AS change_module,
    SUBSTRING(inner_change_type, len(change_address) + len(change_module) + 5) AS change_resource,
    b.value :key :: STRING AS key,
    b.value :value :: STRING AS VALUE,
    b.value :state_key_hash :: STRING AS state_key_hash,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','change_index']) }} AS fact_changes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref(
        'core__fact_transactions'
    ) }} A,
    LATERAL FLATTEN (changes) b

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
