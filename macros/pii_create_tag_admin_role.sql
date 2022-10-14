{% macro pii_create_tag_admin_role() %}
-- dbt run-operation pii_create_tag_admin_role
-- Creates tag_admin role and grants apply masking policy and tag
-- Requires roles securityadmin and accountadmin

{% set sql %}

-- Create role tag_admin
use role securityadmin;
create role if not exists tag_admin;

use role accountadmin;
grant apply masking policy on account to role tag_admin;
grant apply tag on account to role tag_admin;

{% endset %}

{% do run_query(sql) %}

{% do log("Created tag_admin role and Granted apply masking policy and tag", info=True) %}

{% endmacro %}