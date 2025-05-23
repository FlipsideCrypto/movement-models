{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false,
    tags = ['observability']
) }}

WITH summary_stats AS (

    SELECT
        MIN(block_number) AS min_block,
        MAX(block_number) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        {{ ref('core__fact_blocks') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, CURRENT_TIMESTAMP())

{% if is_incremental() %}
AND (
    block_number >= (
        SELECT
            MIN(block_number)
        FROM
            (
                SELECT
                    MIN(block_number) AS block_number
                FROM
                    {{ ref('core__fact_blocks') }}
                WHERE
                    block_timestamp BETWEEN DATEADD('hour', -96, CURRENT_TIMESTAMP())
                    AND DATEADD('hour', -95, CURRENT_TIMESTAMP())
                UNION
                SELECT
                    MIN(VALUE) - 1 AS block_number
                FROM
                    (
                        SELECT
                            blocks_impacted_array
                        FROM
                            {{ this }}
                            qualify ROW_NUMBER() over (
                                ORDER BY
                                    test_timestamp DESC
                            ) = 1
                    ),
                    LATERAL FLATTEN(
                        input => blocks_impacted_array
                    )
            )
    ) {% if var('OBSERV_FULL_TEST') %}
        OR block_number >= 0
    {% endif %}
)
{% endif %}
),
base_blocks AS (
    SELECT
        block_number,
        tx_count AS transaction_count
    FROM
        {{ ref('core__fact_blocks') }}
    WHERE
        block_number BETWEEN (
            SELECT
                min_block
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_block
            FROM
                summary_stats
        )
        AND
            block_number NOT IN (0, 1758, 1760, 1761, 1762, 1763, 1764, 1766)
),
actual_tx_counts AS (
    SELECT
        block_number,
        COUNT(1) AS transaction_count
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_number BETWEEN (
            SELECT
                min_block
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_block
            FROM
                summary_stats
        )
        AND
            block_number NOT IN (0, 1758, 1760, 1761, 1762, 1763, 1764, 1766)
    GROUP BY
        block_number
),
potential_missing_txs AS (
    SELECT
        e.block_number
    FROM
        base_blocks e
        LEFT OUTER JOIN actual_tx_counts A
        ON e.block_number = A.block_number
    WHERE
        COALESCE(
            A.transaction_count,
            0
        ) <> e.transaction_count
),
impacted_blocks AS (
    SELECT
        COUNT(1) AS blocks_impacted_count,
        ARRAY_AGG(block_number) within GROUP (
            ORDER BY
                block_number
        ) AS blocks_impacted_array
    FROM
        potential_missing_txs
)
SELECT
    'transactions' AS test_name,
    min_block,
    max_block,
    min_block_timestamp,
    max_block_timestamp,
    blocks_tested,
    blocks_impacted_count,
    blocks_impacted_array,
    SYSDATE() AS test_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    summary_stats
    JOIN impacted_blocks
    ON 1 = 1
