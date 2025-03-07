{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash', 'change_index'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],    
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['core'],
    enabled = false
) }}

SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    version,
    success,
    A.tx_type,
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
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','change_index']
    ) }} AS changes_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'core__fact_transactions'
    ) }} A,
    LATERAL FLATTEN (changes) b

{% if is_incremental() %}
WHERE
  A.modified_timestamp >= (
    SELECT
      DATEADD('minute', -15, MAX(modified_timestamp))
    FROM
      {{ this }})
{% endif %}
