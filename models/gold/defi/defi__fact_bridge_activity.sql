{{ config(
    materialized = 'incremental',
    unique_key = 'fact_bridge_activity_id',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,version,tx_sender, sender, receiver);",
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} },
    tags = ['noncore']
) }}

{% if execute %}

{% if is_incremental() %}
{% set query %}
CREATE
OR REPLACE temporary TABLE defi.bridge__mod_intermediate_tmp AS

SELECT
    platform,
    MAX(modified_timestamp) modified_timestamp
FROM
    {{ this }}
GROUP BY
    platform {% endset %}
    {% do run_query(
        query
    ) %}
    {% set min_block_date_query %}
SELECT
    MIN(block_timestamp)
FROM
    {{ ref('silver__bridge_layerzero_transfers') }} A
    LEFT JOIN defi.bridge__mod_intermediate_tmp b
    ON A.platform = b.platform
WHERE
    (
        A.modified_timestamp >= b.modified_timestamp
        OR b.modified_timestamp IS NULL
    ) {% endset %}
    {% set min_bd = run_query(min_block_date_query) [0] [0] %}
    {% if not min_bd or min_bd == 'None' %}
        {% set min_bd = '2099-01-01' %}
    {% endif %}
{% endif %}
{% endif %}
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    platform,
    bridge_address,
    event_name,
    direction,
    tx_sender,
    sender,
    receiver,
    source_chain_id,
    source_chain_name,
    destination_chain_id,
    destination_chain_name,
    token_address,
    amount_unadj,
    event_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS fact_bridge_activity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__bridge_layerzero_transfers') }}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= '{{min_bd}}'
{% endif %}