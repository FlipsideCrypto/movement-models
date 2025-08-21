{{ config(
    materialized = 'incremental',
    unique_key = "bridge_layerzero_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, sender, receiver);",
    tags = ['noncore']
) }}

WITH evnts AS (

    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        event_address,
        event_resource,
        event_data,
        inserted_timestamp
    FROM
        {{ ref('core__fact_events') }}
    WHERE
        -- Movement Network bridge events - LayerZero OFT and legacy bridge
        (
            -- LayerZero OFT events for MOVE, USDC.e, USDT.e, WETH.e, WBTC.e
            (
                event_address IN (
                    '0x7e4fd97ef92302eea9b10f74be1d96fb1f1511cf7ed28867b0144ca89c6ebc3c', -- MOVE
                    '0x4d2969d384e440db9f1a51391cfc261d1ec08ee1bdf7b9711a6c05d485a4110a', -- USDC.e
                    '0x38cdb3f0afabee56a3393793940d28214cba1f5781e13d5db18fa7079f60ab55', -- USDT.e
                    '0x3dfe1ac4574c7dbbe6f1c5ba862de88fc3e7d3cf8eba95ef1abf32b582889e6d', -- WETH.e
                    '0xbdf86868a32dbae96f2cd50ab05b4be43b92e84e793a4fc01b5b460cc38fdc14'  -- WBTC.e
                )
                AND event_module = 'oft_core'
                AND event_resource IN ('OftSent', 'OftReceived')
            )
            OR
            -- LayerZero deposit events via bridge_receiver
            (
                event_address = '0x8110d118c4886c48979966b837bbd948f9a35660f36903926f51b93c1da5d1d'
                AND event_module = 'bridge_receiver'
                AND event_resource = 'LZDepositEvent'
            )
            OR
            -- Legacy bridge events
            (
                event_address = '0xf3a5e4355c5ad7f9164da03754b1c90ed3c7f3a611f87d9c220a728e6d71d595'
                AND event_module = 'bridge'
                AND event_resource IN ('BridgeEvent', 'WithdrawEvent')
            )
        )
        AND success

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-11-01'
{% endif %}
),
txs AS (
    SELECT
        block_timestamp,
        tx_hash,
        sender,
        payload :type_arguments [0] :: STRING AS token_address,
        payload :arguments [1] :: STRING AS src_sender
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        success

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-11-01'
{% endif %}
),
chngs AS (
    SELECT
        block_timestamp,
        tx_hash,
        change_module,
        CASE
            WHEN change_module = 'coin' THEN change_data :coin :value
            WHEN change_module = 'oft' THEN COALESCE(change_data :locked_coin :value, change_data :fungible_asset :value)
            WHEN change_module = 'fungible_asset' THEN change_data :fungible_asset :value
        END :: INT AS amount,
        change_resource :: STRING AS token_address,
        change_index
    FROM
        {{ ref('core__fact_changes') }}
    WHERE
        success
        AND change_module IN (
            'coin',
            'oft',
            'fungible_asset'
        ) {# AND amount IS NOT NULL #}

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-11-01'
{% endif %}
),
chngs_2 AS (
    SELECT
        block_timestamp,
        tx_hash,
        token_address
    FROM
        chngs
    WHERE
        change_module = 'coin'
        AND token_address LIKE 'CoinInfo%' qualify(ROW_NUMBER() over(PARTITION BY tx_hash
    ORDER BY
        change_index DESC) = 1)
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    'layerzero' AS platform,
    A.event_address AS bridge_address,
    A.event_resource AS event_name,
    CASE
        WHEN event_resource IN ('OftSent', 'WithdrawEvent') THEN 'outbound'
        WHEN event_resource IN ('OftReceived', 'LZDepositEvent', 'BridgeEvent') THEN 'inbound'
        ELSE 'unknown'
    END AS direction,
    b.sender AS tx_sender,
    CASE
        WHEN event_resource IN ('OftSent', 'WithdrawEvent') THEN b.sender
        WHEN event_resource = 'OftReceived' THEN COALESCE(b.src_sender, event_data:from_address::STRING)
        WHEN event_resource = 'LZDepositEvent' THEN REPLACE(event_data:sender::STRING, '000000000000000000000000', '')
        WHEN event_resource = 'BridgeEvent' THEN event_data:from_address::STRING
        ELSE b.sender
    END AS sender,
    CASE
        WHEN event_resource = 'OftSent' THEN event_data:from_address::STRING
        WHEN event_resource = 'OftReceived' THEN event_data:to_address::STRING
        WHEN event_resource = 'LZDepositEvent' THEN NULL -- receiver is embedded in message
        WHEN event_resource = 'BridgeEvent' THEN event_data:to_address::STRING  
        WHEN event_resource = 'WithdrawEvent' THEN event_data:to_address::STRING
        ELSE COALESCE(
            event_data:receiver::STRING,
            REPLACE(event_data:dst_receiver::STRING, '000000000000000000000000', '')
        )
    END AS receiver,
    CASE
        WHEN direction = 'outbound' THEN 108
        WHEN event_resource = 'OftReceived' THEN 
            CASE 
                WHEN event_data:src_eid::INT = 30101 THEN 1  -- LayerZero Ethereum mainnet EID
                ELSE event_data:src_eid::INT 
            END
        WHEN event_resource = 'LZDepositEvent' THEN 
            CASE 
                WHEN event_data:src_eid::INT = 30101 THEN 1  -- LayerZero Ethereum mainnet EID
                ELSE event_data:src_eid::INT 
            END
        WHEN event_resource = 'BridgeEvent' THEN event_data:source_chain::INT
        ELSE COALESCE(event_data:src_chain_id::INT, event_data:src_eid::INT)
    END AS source_chain_id,
    CASE
        WHEN source_chain_id = 1 THEN 'ethereum'
        WHEN source_chain_id = 108 THEN 'movement'
        ELSE 'unknown'
    END AS source_chain_name,
    CASE
        WHEN direction = 'inbound' THEN 108
        WHEN event_resource = 'OftSent' THEN 
            CASE 
                WHEN event_data:dst_eid::INT = 30101 THEN 1  -- LayerZero Ethereum mainnet EID
                ELSE event_data:dst_eid::INT 
            END
        WHEN event_resource = 'WithdrawEvent' THEN event_data:target_chain::INT
        ELSE COALESCE(event_data:dst_chain_id::INT, event_data:dst_eid::INT)
    END AS destination_chain_id,
    CASE
        WHEN destination_chain_id = 1 THEN 'ethereum'
        WHEN destination_chain_id = 108 THEN 'movement'
        ELSE 'unknown'
    END AS destination_chain_name,
    A.event_address AS token_address,
    CASE
        WHEN event_resource IN ('OftSent', 'OftReceived') THEN COALESCE(event_data:amount_sent_ld::INT, event_data:amount_received_ld::INT)
        WHEN event_resource IN ('BridgeEvent', 'WithdrawEvent') THEN event_data:amount::INT
        ELSE COALESCE(event_data:amount::INT, event_data:amount_ld::INT)
    END AS amount_unadj,
    A.event_index,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash','a.event_index']
    ) }} AS bridge_layerzero_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
    JOIN txs b
    ON A.tx_hash = b.tx_hash
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
    LEFT JOIN chngs C
    ON A.tx_hash = C.tx_hash
    AND A.block_timestamp :: DATE = C.block_timestamp :: DATE
    AND amount_unadj = C.amount
    AND C.change_module = 'coin'
    LEFT JOIN chngs d
    ON A.tx_hash = d.tx_hash
    AND A.block_timestamp :: DATE = d.block_timestamp :: DATE
    AND d.change_module IN ('oft', 'fungible_asset')
    AND d.amount IS NOT NULL
    LEFT JOIN chngs_2 e
    ON A.tx_hash = e.tx_hash
    AND A.block_timestamp :: DATE = e.block_timestamp :: DATE
    AND C.tx_hash IS NULL
    AND d.tx_hash IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.tx_hash, a.event_index ORDER BY a.block_number DESC) = 1