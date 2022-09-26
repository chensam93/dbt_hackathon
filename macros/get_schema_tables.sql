{% macro get_schema_tables(sch_name) %}
    select table_schema,
        table_name,
        table_type,
        row_count
    from information_schema.tables
    where table_type = 'BASE TABLE'
        and row_count is not null
        and row_count > 0
        and table_name not like 'FIVETRAN%'
        and table_name != 'PII_COLUMNS'
        and table_schema = '{{sch_name}}'
    order by 1,2
{% endmacro %}