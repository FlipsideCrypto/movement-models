{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"evm_traces",
        "sql_limit" :"25000",
        "producer_batch_size" :"1000",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result"]) }
    ),
    tags = ['streamline_core_evm_realtime']
) }}

WITH to_do AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__evm_blocks") }}
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_evm_blocks") }}
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
)
SELECT
    block_number,
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'User-Agent',
            'Flipside_Crypto/0.1'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number :: STRING,
            'jsonrpc',
            '2.0',
            'method',
            'debug_traceBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '30s'))
        ),
        'Vault/prod/m1/evm'
    ) AS request
FROM
    ready_blocks
ORDER BY
    block_number DESC