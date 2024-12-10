{% macro create_aws_movement_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_movement_api_prod_v2 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/movement-api-prod-rolesnowflakeudfsAF733095-uYaV9o2uJLDU' api_allowed_prefixes = (
            'https://d0t060jjxf.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_movement_api_stg_v2 api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/movement-api-stg-rolesnowflakeudfsAF733095-XfjrcIZRSZkR' api_allowed_prefixes = (
            'https://qjj5rutl05.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
