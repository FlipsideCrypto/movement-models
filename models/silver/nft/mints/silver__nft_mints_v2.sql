{{ config(
    materialized = 'incremental',
    unique_key = "nft_mints_v2_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE','inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

{% if execute %}

{% if is_incremental() %}
{% set min_bts_query %}

SELECT
    MIN(block_timestamp) :: DATE
FROM
    {{ ref('core__fact_events') }}
WHERE
    _inserted_timestamp > GREATEST(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        SYSDATE() :: DATE - 3
    ) {% endset %}
    {% set min_bts = run_query(min_bts_query) [0] [0] %}
    {% if not min_bts or min_bts == 'None' %}
        {% set min_bts = '2099-01-01' %}
    {% endif %}
{% endif %}
{% endif %}

WITH evnts AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        sender as account_address,
        event_address,
        event_resource,
        event_data,
        event_module,
        event_type,
        inserted_timestamp
    FROM
        {{ ref('core__fact_events') }}
    WHERE
        success
        AND event_type IN (
            '0x4::collection::Mint',
            '0x1::coin::WithdrawEvent'
        )
    {% if is_incremental() %}
    AND inserted_timestamp >= GREATEST(
    (
        SELECT
            MAX(inserted_timestamp)
        FROM
            {{ this }}
    ),
    SYSDATE() :: DATE - 3
)
{% endif %}
),
chngs AS (
    SELECT
        block_timestamp,
        tx_hash,
        change_data,
        address,
        inner_change_type,
        change_resource
    FROM
        {{ ref('core__fact_changes') }}
    WHERE
        success
        AND (
            inner_change_type IN (
                '0x4::collection::Collection',
                '0x4::token::Token'
            )
        )
    {% if is_incremental() %}
    AND inserted_timestamp >= GREATEST(
    (
        SELECT
            MAX(inserted_timestamp)
        FROM
            {{ this }}
    ),
    SYSDATE() :: DATE - 3
)
{% endif %}
),
xfers AS (
    SELECT
        tx_hash,
        account_address,
        transfer_event,
        token_address
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        success
    {% if is_incremental() %}
    AND inserted_timestamp >= GREATEST(
        (
            SELECT
                MAX(inserted_timestamp)
            FROM
                {{ this }}
        ),
        SYSDATE() :: DATE - 3
    )
    {% endif %}
    UNION ALL
    SELECT
        tx_hash,
        owner_address AS account_address,
        transfer_event,
        metadata_address AS token_address
    FROM
        {{ ref('silver__transfers_fungible') }}
    WHERE
        success
    {% if is_incremental() %}
    AND inserted_timestamp >= GREATEST(
    (
        SELECT
            MAX(inserted_timestamp)
        FROM
            {{ this }}
    ),
    SYSDATE() :: DATE - 3
)
{% endif %}
),
v2_mint_events_raw AS (
    SELECT
        *,
        'v2' AS token_version,
        LAG(
            event_index,
            1,
            0
        ) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS prev_event_index,
        LEAD(
            event_index,
            1,
            0
        ) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS next_event_index_raw,
        1 AS nft_count,
        event_data :token :: STRING AS nft_address,
        event_data :collection :: STRING as collection_address
    FROM
        evnts
    WHERE
        event_type = '0x4::collection::Mint'
),
mint_collection_names_v2 AS (
    SELECT
        *
    FROM
        chngs
    WHERE
        inner_change_type = '0x4::collection::Collection'
),
mint_token_names_v2 AS (
    SELECT
        *
    FROM
        chngs
    WHERE
        inner_change_type = '0x4::token::Token'
),
v2_mints_with_project_name AS (
    SELECT
        main.*,
        collection.change_data :name AS project_name
    FROM
        v2_mint_events_raw main
        LEFT JOIN mint_collection_names_v2 collection
        ON main.tx_hash = collection.tx_hash
        AND main.collection_address = collection.address
    GROUP BY
        ALL
),
v2_mints_with_token_id_raw AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                token.change_data :name AS tokenid,
                MIN(
                    main.event_index
                ) over (
                    PARTITION BY main.tx_hash
                    ORDER BY
                        main.event_index
                ) AS min_mint_index
            FROM
                v2_mints_with_project_name main
                JOIN mint_token_names_v2 token
                ON main.tx_hash = token.tx_hash
                AND main.nft_address = token.address
        )
    GROUP BY
        ALL
),
coin_withdraw_events_v2 AS (
    SELECT
        *,
        MIN(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index
        ) AS min_with_index,
        MAX(event_index) over (
            PARTITION BY tx_hash
            ORDER BY
                event_index DESC
        ) AS max_with_index
    FROM
        evnts
    WHERE
        event_type = '0x1::coin::WithdrawEvent'
),
v2_mints_with_token_id AS (
    SELECT
        *,
        (
            CASE
                WHEN next_event_index_raw = 0 THEN max_with_index
                ELSE next_event_index_raw
            END
        ) AS next_event_index
    FROM
        (
            SELECT
                main.*,
                withdraw.max_with_index
            FROM
                v2_mints_with_token_id_raw main
                LEFT JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
            GROUP BY
                ALL
        )
),
v2_mint_events_with_price AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                withdraw.event_index AS with_ev_index,
                withdraw.event_data :amount :: NUMBER AS withdraw_amount
            FROM
                v2_mint_events_raw main
                LEFT JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
                AND main.prev_event_index <= withdraw.event_index
                AND main.event_index > withdraw.event_index
        )
),
mint_counts_per_tx AS (
    SELECT
        tx_hash,
        COUNT(tx_hash) AS tx_hash_count
    FROM
        v2_mints_with_token_id
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                xfers
        )
    GROUP BY
        tx_hash
),
withdrawal_counts_per_tx AS (
    SELECT
        tx_hash,
        COUNT(tx_hash) AS tx_hash_count
    FROM
        coin_withdraw_events_v2
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                xfers
        )
    GROUP BY
        tx_hash
),
joined_mint_with_count AS (
    SELECT
        mint.tx_hash,
        mint.tx_hash_count AS mint_tx_count,
        withdrawal.tx_hash_count AS withdrawal_tx_count
    FROM
        mint_counts_per_tx mint
        JOIN withdrawal_counts_per_tx withdrawal
        ON mint.tx_hash = withdrawal.tx_hash
    ORDER BY
        mint_tx_count
),
mints_v2_with_price_raw1 AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                SUM(
                    withdraw.event_data :amount :: NUMBER
                ) over (
                    PARTITION BY main.tx_hash,
                    main.event_index
                    ORDER BY
                        withdraw.event_index
                ) AS price_raw
            FROM
                v2_mints_with_token_id main
                JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
                AND main.event_index < withdraw.event_index
                AND main.next_event_index >= withdraw.event_index
            WHERE
                main.tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        xfers
                )
                AND main.tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        joined_mint_with_count
                    WHERE
                        withdrawal_tx_count = 1
                        AND withdrawal_tx_count < mint_tx_count
                )
                AND main.min_mint_index < withdraw.min_with_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                price_raw DESC
        ) = 1
),
mints_v2_with_price_raw2 AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                SUM(
                    withdraw.event_data :amount :: NUMBER
                ) over (
                    PARTITION BY main.tx_hash,
                    main.event_index
                    ORDER BY
                        withdraw.event_index
                ) AS price_raw
            FROM
                v2_mints_with_token_id main
                JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
                AND main.event_index > withdraw.event_index
                AND main.prev_event_index <= withdraw.event_index
            WHERE
                main.tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        xfers
                )
                AND main.tx_hash NOT IN (
                    SELECT
                        tx_hash
                    FROM
                        joined_mint_with_count
                    WHERE
                        withdrawal_tx_count = 1
                        AND withdrawal_tx_count < mint_tx_count
                )
                AND main.min_mint_index > withdraw.min_with_index
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                price_raw DESC
        ) = 1
),
mints_v2_with_price_raw3 AS (
    SELECT
        *
    FROM
        (
            SELECT
                main.*,
                withdraw.event_data :amount :: NUMBER / tx_count.mint_tx_count AS price_raw
            FROM
                v2_mints_with_token_id main
                JOIN coin_withdraw_events_v2 withdraw
                ON main.tx_hash = withdraw.tx_hash
                JOIN joined_mint_with_count tx_count
                ON main.tx_hash = tx_count.tx_hash
            WHERE
                main.tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        xfers
                )
                AND main.tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        joined_mint_with_count
                    WHERE
                        withdrawal_tx_count = 1
                        AND withdrawal_tx_count < mint_tx_count
                )
        ) qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                price_raw DESC
        ) = 1
),
mints_v2_with_price AS (
    SELECT
        *
    FROM
        mints_v2_with_price_raw3
    UNION
    SELECT
        *
    FROM
        mints_v2_with_price_raw2
    UNION
    SELECT
        *
    FROM
        mints_v2_with_price_raw1
),
mints_v2_with_no_price AS (
    SELECT
        *,
        0 AS price_raw
    FROM
        v2_mints_with_token_id
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                xfers
        )
),
fin AS (
    SELECT
        *
    FROM
        mints_v2_with_no_price
    UNION ALL
    SELECT
        *
    FROM
        mints_v2_with_price
)
SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_resource AS event_type,
    nft_address :: STRING AS nft_address,
    project_name :: STRING AS project_name,
    event_address AS nft_from_address,
    account_address AS nft_to_address,
    tokenid :: STRING AS tokenid,
    token_version,
    nft_count,
    price_raw,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS nft_mints_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin