{% macro pii_create_execution_role() %}
-- dbt run-operation pii_create_execution_role
-- Creates execution role, grants privileges to role, and assigns users to role
-- Requires roles securityadmin

{% set sql %}

-- Set variables for admin role, db, schema, warehouse
set this_role = '{{ var("pii_execution_role") }}';
set this_db = '{{ var("pii_database") }}';
set this_schema = '{{ var("pii_schema") }}';
set this_warehouse = '{{ var("pii_warehouse") }}';
set this_db_schema = concat($this_db, '.', $this_schema);

-- Assign grants to execution role
use role securityadmin;
create role if not exists identifier($this_role);
grant operate on warehouse identifier($this_warehouse) to role identifier($this_role);
grant usage on warehouse identifier($this_warehouse) to role identifier($this_role);
grant usage on database identifier($this_db) to role identifier($this_role);
grant usage on all schemas in database identifier($this_db) to role identifier($this_role);
grant role tag_admin to role identifier($this_role);

-- Assign schema level grants
grant all privileges on schema identifier($this_db_schema) to role identifier($this_role);
grant all privileges on all tables in schema identifier($this_db_schema) to role identifier($this_role);
grant all privileges on future tables in schema identifier($this_db_schema) to role identifier($this_role);
grant all privileges on all views in schema identifier($this_db_schema) to role identifier($this_role);
grant all privileges on future views in schema identifier($this_db_schema) to role identifier($this_role);

-- Assign role to users
{% for role_user in var('pii_execution_role_users') %}
grant role identifier($this_role) to user {{role_user}};
{% endfor %}

{% endset %}

{% do run_query(sql) %}

{% do log("Created execution role, granted privileges to role, and assigned users to role", info=True) %}

{% endmacro %}