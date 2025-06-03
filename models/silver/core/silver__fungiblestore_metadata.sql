{{ config(
  materialized = 'incremental',
  unique_key = ['store_address'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp","block_timestamp_first","block_number_first"],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(store_address);",
  tags = ['core']
) }}

-- depends_on: {{ ref('core__fact_changes') }}

SELECT
  block_timestamp AS block_timestamp_first,
  block_number AS block_number_first,
  address AS store_address,
  change_data :metadata :inner :: STRING AS metadata_address,
  {{ dbt_utils.generate_surrogate_key(
    ['store_address']
  ) }} AS fungiblestore_metadata_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  {{ ref('core__fact_changes') }}
WHERE
  change_module = 'fungible_asset'
  AND change_resource = 'FungibleStore'

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
  PARTITION BY address
  ORDER BY
    block_number
) = 1
