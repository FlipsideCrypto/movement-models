{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash', 'block_timestamp::DATE'],
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
  merge_exclude_columns = ['inserted_timestamp'],
  cluster_by = ['modified_timestamp::DATE'],
  tags = ['core']
) }}
-- depends_on: {{ ref('bronze__transactions') }}
WITH from_transactions AS (

  SELECT
    value :BLOCK_NUMBER :: bigint AS block_number,
    TO_TIMESTAMP(
      VALUE :BLOCK_TIMESTAMP :: STRING
    ) AS block_timestamp,
    DATA :hash :: STRING AS tx_hash,
    DATA :version :: INT AS version,
    DATA :type :: STRING AS tx_type,
    DATA,
    inserted_timestamp AS file_last_updated
  FROM
  {% if is_incremental() %}
    {{ ref('bronze__transactions') }}
  {% else %}
    {{ ref('bronze__transactions_FR') }}
  {% endif %}

  WHERE
    version BETWEEN VALUE :FIRST_VERSION :: bigint
    AND VALUE :LAST_VERSION :: bigint

  {% if is_incremental() %}
  AND inserted_timestamp >= (
    SELECT
      DATEADD('minute', -5, MAX(modified_timestamp))
    FROM
      {{ this }})
    {% endif %}
),
transformed AS (
  SELECT
    block_number,
    COALESCE(
      block_timestamp,
      '1970-01-01 00:00:00.000'
    ) AS block_timestamp,
    tx_hash,
    version,
    tx_type,
    DATA :success :: BOOLEAN AS success,
    DATA :epoch :: INT AS epoch,
    DATA :sender :: STRING AS sender,
    DATA :signature :: STRING AS signature,
    DATA :payload AS payload,
    DATA :payload :function :: STRING AS payload_function,
    DATA :changes AS changes,
    DATA :events AS events,
    DATA :gas_unit_price :: bigint AS gas_unit_price,
    DATA :gas_used :: INT AS gas_used,
    DATA :max_gas_amount :: bigint AS max_gas_amount,
    DATA :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs,
    DATA :vm_status :: STRING AS vm_status,
    DATA :state_change_hash :: STRING AS state_change_hash,
    DATA :accumulator_root_hash :: STRING AS accumulator_root_hash,
    DATA :event_root_hash :: STRING AS event_root_hash,
    DATA :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
    DATA :failed_proposer_indices AS failed_proposer_indices,
    DATA :id :: STRING AS id,
    DATA :previous_block_votes_bitvec AS previous_block_votes_bitvec,
    DATA :proposer :: STRING AS proposer,
    DATA :round :: INT AS round,
    DATA,
    file_last_updated
  FROM
    from_transactions
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  version,
  tx_type,
  success,
  sender,
  signature,
  payload,
  payload_function,
  changes,
  events,
  gas_unit_price,
  gas_used,
  max_gas_amount,
  expiration_timestamp_secs,
  vm_status,
  state_change_hash,
  accumulator_root_hash,
  event_root_hash,
  state_checkpoint_hash,
  failed_proposer_indices,
  id,
  previous_block_votes_bitvec,
  proposer,
  ROUND,
  DATA,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash']
  ) }} AS transactions_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  transformed qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
  file_last_updated DESC)) = 1
