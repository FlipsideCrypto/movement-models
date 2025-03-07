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
-- depends_on: {{ ref('core__fact_transactions') }}
SELECT
    A.block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    tx_type,
    payload_function,
    index AS change_index,
    DATA :data AS change_data,
    DATA :type :: STRING AS change_type,
    DATA :address :: STRING AS address,
    DATA :handle :: STRING AS handle,
    DATA :"type" :: STRING AS inner_change_type,
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
    VALUE :key :: STRING AS key,
    VALUE :value :: STRING AS VALUE,
    VALUE :state_key_hash :: STRING AS state_key_hash,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','change_index']
    ) }} AS changes_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref(
        'core__fact_transactions'
    ) }}

{% if is_incremental() %}
WHERE
  A.modified_timestamp >= (
    SELECT
      DATEADD('minute', -15, MAX(modified_timestamp))
    FROM
      {{ this }})
{% endif %}
