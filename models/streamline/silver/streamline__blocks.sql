{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    0 AS block_number
UNION ALL
SELECT
    _id AS block_number
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            MAX(block_number)
        FROM
            {{ ref('streamline__chainhead') }}
    )
