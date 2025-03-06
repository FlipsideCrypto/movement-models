{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks_tx",
        "sql_limit" :"8000",
        "producer_batch_size" :"50",
        "worker_batch_size" :"400",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_number"}
    ),
    tags = ['streamline_core_realtime']
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref('streamline__blocks') }}
    WHERE
        block_number != 0
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref('streamline__blocks_tx_complete') }}
)
SELECT
    block_number,
    ROUND(
        block_number,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/v1/blocks/by_height/' || block_number || '?with_transactions=true',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'User-Agent',
            'Flipside_Crypto/0.1'
        ),
        PARSE_JSON('{}'),
        'Vault/prod/movement/mainnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
