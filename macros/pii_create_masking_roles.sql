{% macro pii_create_masking_roles() %}
-- dbt run-operation pii_create_masking_roles
-- Creates masking roles, grants privileges to each role, and assigns users to each role
-- Requires roles securityadmin

{% set sql %}

{% for masking_role in var('pii_masking_roles') %}

-- Set variables for masking_role, db, schema, warehouse
set this_role = '{{masking_role}}';
set this_db = '{{ var("pii_database") }}';
set this_schema = '{{ var("pii_schema") }}';
set this_warehouse = '{{ var("pii_warehouse") }}';
set this_db_schema = concat($this_db, '.', $this_schema);

-- Assign grants to masking_role
use role securityadmin;
create role if not exists identifier($this_role);
grant operate on warehouse identifier($this_warehouse) to role identifier($this_role);
grant usage on warehouse identifier($this_warehouse) to role identifier($this_role);
grant usage on database identifier($this_db) to role identifier($this_role);
grant usage on all schemas in database identifier($this_db) to role identifier($this_role);

-- Schema grants
grant usage on schema identifier($this_db_schema) to role identifier($this_role);
grant select on all tables in schema identifier($this_db_schema) to role identifier($this_role);
grant select on future tables in schema identifier($this_db_schema) to role identifier($this_role);
grant select on all views in schema identifier($this_db_schema) to role identifier($this_role);
grant select on future views in schema identifier($this_db_schema) to role identifier($this_role);

-- Assign role to users
{% for role_user in var('pii_masking_role_users') %}
grant role identifier($this_role) to user {{role_user}};
{% endfor %}

-- End masking role
{% endfor %}

{% endset %}

{% do run_query(sql) %}

{% do log("Created masking roles, granted privileges to roles, and assigned users to roles", info=True) %}

{% endmacro %}