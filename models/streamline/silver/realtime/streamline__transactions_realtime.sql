{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"transactions",
        "sql_limit" :"50000",
        "producer_batch_size" :"5000",
        "worker_batch_size" :"5000",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["data"]),
        "order_by_column": "block_number" }
    ),
    tags = ['streamline_core_realtime']
) }}

WITH blocks AS (

    SELECT
        A.block_number,
        tx_count_from_versions AS tx_count,
        first_version,
        last_version,
        block_timestamp
    FROM
        {{ ref('streamline__blocks_tx_complete') }} A
    WHERE
        block_number <> 0
),
numbers AS (
    SELECT
        1 AS n
    UNION ALL
    SELECT
        n + 1
    FROM
        numbers
    WHERE
        n < (
            SELECT
                CEIL(MAX(tx_count) / 100.0)
            FROM
                blocks)
        ),
        blocks_with_page_numbers AS (
            SELECT
                tt.block_number,
                n.n - 1 AS multiplier,
                first_version,
                last_version,
                tx_count,
                block_timestamp
            FROM
                blocks tt
                JOIN numbers n
                ON n.n <= CASE
                    WHEN tt.tx_count % 100 = 0 THEN tt.tx_count / 100
                    ELSE FLOOR(
                        tt.tx_count / 100
                    ) + 1
                END
        ),
        WORK AS (
            SELECT
                A.block_number,
                block_timestamp,
                first_version,
                last_version,
                first_version +(
                    100 * multiplier
                ) AS tx_version,
                multiplier,
                LEAST (
                    tx_count - 100 * multiplier,
                    100
                ) AS lim,
                tx_count
            FROM
                blocks_with_page_numbers A
                LEFT JOIN {{ ref('streamline__transactions_complete') }}
                b
                ON A.block_number = b.block_number
                AND multiplier = b.multiplier_no
            WHERE
                b.block_number IS NULL
        )
    SELECT
        block_number,
        block_timestamp,
        first_version,
        last_version,
        tx_version,
        multiplier,
        ROUND(
            block_number,
            -4
        ) :: INT AS partition_key,
        {{ target.database }}.live.udf_api(
            'GET',
            '{Service}/v1/transactions?start=' || tx_version || '&limit=' || lim,
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'User-Agent',
                'Flipside_Crypto/0.1'
            ),
            PARSE_JSON('{}'),
            'Vault/prod/movement/mainnet_fsc'
        ) AS request
    FROM
        WORK
