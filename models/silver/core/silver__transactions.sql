{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash', 'block_timestamp::DATE'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],    
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['core', 'full_test']
) }}

-- depends_on: {{ ref('bronze__blocks_tx') }}
-- depends_on: {{ ref('bronze__transactions') }}

WITH from_blocks AS (
  SELECT
    TO_TIMESTAMP(
      b.value :timestamp :: STRING
    ) AS block_timestamp,
    b.value :hash :: STRING AS tx_hash,
    b.value :version :: INT AS version,
    b.value :type :: STRING AS tx_type,
    b.value AS DATA,
    inserted_timestamp AS file_last_updated
  FROM
  {% if is_incremental() %}
    {{ ref('bronze__blocks_tx') }}
  {% else %}
    {{ ref('bronze__blocks_tx_FR') }}
  {% endif %}
  A,
  LATERAL FLATTEN (DATA :transactions) b

  {% if is_incremental() %}
  WHERE
    A.inserted_timestamp >= (
      SELECT
        DATEADD('minute', -15, MAX(modified_timestamp))
      FROM
        {{ this }})
  {% endif %}
),
from_transactions AS (
  SELECT
    TO_TIMESTAMP(
      b.value :timestamp :: STRING
    ) AS block_timestamp,
    b.value :hash :: STRING AS tx_hash,
    b.value :version :: INT AS version,
    b.value :type :: STRING AS tx_type,
    b.value AS DATA,
    inserted_timestamp AS file_last_updated
  FROM
  {% if is_incremental() %}
    {{ ref('bronze__transactions') }}
  {% else %}
    {{ ref('bronze__transactions_FR') }}
  {% endif %}
  A,
  LATERAL FLATTEN(A.data) b
  {% if is_incremental() %}
  WHERE
    A.inserted_timestamp >= (
      SELECT
        DATEADD('minute', -15, MAX(modified_timestamp))
      FROM
        {{ this }})
  {% endif %}
),
combo AS (
  SELECT
    block_timestamp,
    tx_hash,
    version,
    tx_type,
    DATA,
    file_last_updated
  FROM
    from_blocks
  UNION ALL
  SELECT
    block_timestamp,
    tx_hash,
    version,
    tx_type,
    DATA,
    file_last_updated
  FROM
    from_transactions A
),
transformed AS (
  SELECT
    COALESCE(
      block_timestamp,
      '1970-01-01 00:00:00.000'
    ) AS block_timestamp,
    tx_hash,
    version,
    tx_type,
    DATA :success :: BOOLEAN AS success,
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
    DATA :failed_proposer_indices :: STRING AS failed_proposer_indices,
    DATA :id :: STRING AS id,
    DATA :previous_block_votes_bitvec :: STRING AS previous_block_votes_bitvec,
    DATA :proposer :: STRING AS proposer,
    DATA :ROUND :: INT AS ROUND,
    DATA,
    file_last_updated
  FROM
    combo
)
SELECT
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
