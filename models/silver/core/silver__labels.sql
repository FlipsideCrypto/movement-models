{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    system_created_at::TIMESTAMP_NTZ AS system_created_at,
    insert_date::DATE AS insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    modified_timestamp
FROM
    {{ source(
        'crosschain',
        'dim_labels'
    ) }}
WHERE
    blockchain = 'movement'