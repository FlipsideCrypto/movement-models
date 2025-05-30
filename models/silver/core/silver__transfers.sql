{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','event_index','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  tags = ['core']
) }}

WITH events AS (

  SELECT
    block_number,
    version,
    success,
    block_timestamp,
    block_timestamp :: DATE AS block_date,
    tx_hash,
    event_index,
    event_resource,
    event_data :amount :: bigint AS amount,
    account_address,
    creation_number,
    modified_timestamp
  FROM
    {{ ref(
      'core__fact_events'
    ) }}
  WHERE
    event_module = 'coin'
    AND event_resource IN (
      'WithdrawEvent',
      'DepositEvent'
    )

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
changes AS (
  SELECT
    block_timestamp :: DATE AS block_date,
    tx_hash,
    change_index,
    change_data,
    change_data :deposit_events :guid :id :creation_num :: INT AS creation_number_deposit,
    change_data :withdraw_events :guid :id :creation_num :: INT AS creation_number_withdraw,
    address,
    change_resource AS token_address
  FROM
    {{ ref(
      'core__fact_changes'
    ) }}
  WHERE
    change_module = 'coin'
    AND (
      creation_number_deposit IS NOT NULL
      OR creation_number_withdraw IS NOT NULL
    )

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
changes_dep AS (
  SELECT
    block_date,
    tx_hash,
    address,
    creation_number_deposit AS creation_number,
    token_address
  FROM
    changes
  WHERE
    creation_number_deposit IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_hash, creation_number_deposit, address
  ORDER BY
    change_index DESC) = 1)
),
changes_wth AS (
  SELECT
    block_date,
    tx_hash,
    address,
    creation_number_withdraw AS creation_number,
    token_address
  FROM
    changes
  WHERE
    creation_number_withdraw IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_hash, creation_number_withdraw, address
  ORDER BY
    change_index DESC) = 1)
)
SELECT
  e.block_number,
  e.block_timestamp,
  e.tx_hash,
  e.version,
  e.success,
  e.event_index,
  e.creation_number,
  e.event_resource AS transfer_event,
  e.account_address,
  e.amount,
  REPLACE(
    REPLACE(
      COALESCE(
        dep.token_address,
        wth.token_address
      ),
      'CoinStore<'
    ),
    '>'
  ) AS token_address,
  {{ dbt_utils.generate_surrogate_key(
    ['e.tx_hash','e.event_index']
  ) }} AS transfers_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  events e
  LEFT JOIN changes_dep dep
  ON e.block_date = dep.block_date
  AND e.tx_hash = dep.tx_hash
  AND e.creation_number = dep.creation_number
  AND e.account_address = dep.address
  AND e.event_resource = 'DepositEvent'
  LEFT JOIN changes_wth wth
  ON e.block_date = wth.block_date
  AND e.tx_hash = wth.tx_hash
  AND e.creation_number = wth.creation_number
  AND e.account_address = wth.address
  AND e.event_resource = 'WithdrawEvent'
WHERE
  COALESCE(
    dep.token_address,
    wth.token_address
  ) IS NOT NULL
