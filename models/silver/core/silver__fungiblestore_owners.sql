{{ config(
  materialized = 'incremental',
  unique_key = ['version','change_index'],
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(store_address);",
  tags = ['core']
) }}
-- depends_on: {{ ref('core__fact_changes') }}

SELECT
  block_timestamp,
  block_number,
  version,
  tx_hash,
  change_index,
  address AS store_address,
  change_data :owner :: STRING AS owner_address,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','change_index']
  ) }} AS fungiblestore_owners_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  {{ ref('core__fact_changes') }}
WHERE
  change_address = '0x1'
  AND change_module = 'object'
  AND change_resource = 'ObjectCore'

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
