{% macro pii_create_results_schema_and_fake_function() %}
-- dbt run-operation pii_create_results_schema_and_fake_function
-- Creates results schema and fake function for faking data
-- Requires roles securityadmin

{% set sql %}

-- Set variables for admin role, db, schema, warehouse
set this_role = '{{ var("pii_execution_role") }}';
set this_db = '{{ var("pii_database") }}';
set this_schema = '{{ var("pii_results_schema") }}';
set this_warehouse = '{{ var("pii_warehouse") }}';
set this_db_schema = concat($this_db, '.', $this_schema);
set this_function = concat($this_db_schema, '.fake');

set target_db = '{{ target.database }}';
set target_schema = '{{ target.schema }}';
set target_db_schema = concat($target_db, '.', $target_schema, '_', $this_schema);

-- Create results schema
use database identifier($this_db);
use role identifier($this_role);
use warehouse identifier($this_warehouse);
create schema if not exists identifier($this_db_schema);
create schema if not exists identifier($target_db_schema);
use schema identifier($this_schema);

-- Create function in results schema to fake data
-- Reference: https://medium.com/snowflake/flaker-2-0-fake-snowflake-data-the-easy-way-dc5e65225a13
create function if not exists fake(locale varchar,provider varchar,parameters variant)
returns variant
language python
volatile
runtime_version = '3.8'
packages = ('faker','simplejson')
handler = 'fake'
as
$$
import simplejson as json
from faker import Faker
def fake(locale,provider,parameters):
  if type(parameters).__name__=='sqlNullWrapper':
    parameters = {}
  fake = Faker(locale=locale)
  return json.loads(json.dumps(fake.format(formatter=provider,**parameters), default=str))
$$;

-- Grant privileges on results schema and function to this role
use role securityadmin;
grant all on schema identifier($this_db_schema) to role identifier($this_role);
grant all on all tables in schema identifier($this_db_schema) to role identifier($this_role);
grant all on future tables in schema identifier($this_db_schema) to role identifier($this_role);
grant usage on function identifier($this_function)(string, string, variant) to role identifier($this_role);
-- Grant privileges on target schema to this role
grant all on schema identifier($target_db_schema) to role identifier($this_role);
grant all on all tables in schema identifier($target_db_schema) to role identifier($this_role);
grant all on future tables in schema identifier($target_db_schema) to role identifier($this_role);

-- Grant privileges on results schema and function to masking roles
{% for masking_role in var('pii_masking_roles') %}
grant all on schema identifier($this_db_schema) to role identifier($this_role);
grant all on all tables in schema identifier($this_db_schema) to role identifier($this_role);
grant all on future tables in schema identifier($this_db_schema) to role identifier($this_role);
grant usage on function identifier($this_function)(string, string, variant) to role {{masking_role}};
-- Grant privileges on target schema to masking roles
grant all on schema identifier($target_db_schema) to role {{masking_role}};
grant all on all tables in schema identifier($target_db_schema) to role {{masking_role}};
grant all on future tables in schema identifier($target_db_schema) to role {{masking_role}};
{% endfor %}

{% endset %}

{% do run_query(sql) %}

{% do log("Created results schema and fake function for faking data", info=True) %}

{% endmacro %}