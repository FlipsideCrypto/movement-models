{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','event_index','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  tags = ['core']
) }}

-- depends_on: {{ ref('core__fact_events') }}
-- depends_on: {{ ref('silver__fungiblestore_owners') }}
-- depends_on: {{ ref('silver__fungiblestore_metadata') }}

{% if execute %}
  {% set base_query %}
  CREATE
  OR REPLACE temporary TABLE silver.transfers_fungible__intermediate_tmp AS

  SELECT
    block_number,
    version,
    success,
    block_timestamp,
    tx_hash,
    event_index,
    event_resource,
    event_data :amount :: bigint AS amount,
    event_data :store :: STRING AS store_address
  FROM
    {{ ref('core__fact_events') }}
  WHERE
    event_module = 'fungible_asset'
    AND event_resource IN (
      'WithdrawEvent',
      'DepositEvent',
      'Withdraw',
      'Deposit'
    )

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}

{% endset %}
{% do run_query(base_query) %}
{% set owner_query %}
CREATE
OR REPLACE temporary TABLE silver.transfers_fungible_store_owners__intermediate_tmp AS
SELECT
  version,
  block_timestamp,
  store_address,
  owner_address,
  COUNT(
    DISTINCT owner_address
  ) over(
    PARTITION BY store_address
  ) owner_cnt
FROM
  {{ ref('silver__fungiblestore_owners') }}
WHERE
  store_address IN (
    SELECT
      store_address
    FROM
      silver.transfers_fungible__intermediate_tmp
  )
ORDER BY
  block_timestamp {% endset %}
  {% do run_query(owner_query) %}
  {% set owner_query_single %}
  CREATE
  OR REPLACE temporary TABLE silver.transfers_fungible_store_owners_single__intermediate_tmp AS
SELECT
  DISTINCT store_address,
  owner_address
FROM
  silver.transfers_fungible_store_owners__intermediate_tmp
WHERE
  owner_cnt = 1 {% endset %}
  {% do run_query(owner_query_single) %}
  {% set owner_query_many %}
  CREATE
  OR REPLACE temporary TABLE silver.transfers_fungible_store_owners_many__intermediate_tmp AS WITH base AS (
    SELECT
      store_address,
      owner_address,
      block_timestamp,
      conditional_change_event(owner_address) over (
        PARTITION BY store_address
        ORDER BY
          block_timestamp
      ) AS change_event,
      ROW_NUMBER() over (
        PARTITION BY store_address
        ORDER BY
          block_timestamp
      ) AS rn
    FROM
      silver.transfers_fungible_store_owners__intermediate_tmp
    WHERE
      owner_cnt > 1
  )
SELECT
  store_address,
  owner_address,
  block_timestamp
FROM
  base qualify ROW_NUMBER() over(
    PARTITION BY store_address,
    change_event
    ORDER BY
      rn
  ) = 1;
{% endset %}
  {% do run_query(owner_query_many) %}
{% endif %}

WITH md AS (
  SELECT
    store_address,
    metadata_address
  FROM
    {{ ref('silver__fungiblestore_metadata') }}
)
SELECT
  e.block_number,
  e.block_timestamp,
  e.tx_hash,
  e.version,
  e.success,
  e.event_index,
  CASE
    WHEN event_resource IN (
      'WithdrawEvent',
      'Withdraw'
    ) THEN 'WithdrawEvent'
    WHEN event_resource IN (
      'DepositEvent',
      'Deposit'
    ) THEN 'DepositEvent'
  END AS transfer_event,
  e.store_address,
  COALESCE(
    os.owner_address,
    om.owner_address
  ) AS owner_address,
  m.metadata_address,
  e.amount,
  os.owner_address os,
  om.owner_address om,
  om.block_timestamp om_block_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['e.tx_hash','e.event_index']
  ) }} AS transfers_fungible_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  silver.transfers_fungible__intermediate_tmp e asof
  JOIN silver.transfers_fungible_store_owners_many__intermediate_tmp om match_condition(
    e.block_timestamp >= om.block_timestamp
  )
  ON e.store_address = om.store_address
  LEFT JOIN silver.transfers_fungible_store_owners_single__intermediate_tmp os
  ON e.store_address = os.store_address
  LEFT JOIN md m
  ON e.store_address = m.store_address
