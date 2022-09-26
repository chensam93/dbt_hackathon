from datetime import timezone
import datetime
import pandas
import pandas as pd
from pandas import DataFrame

# Probability (Confidence) Threhold for tagging PII Columns
PROBABILITY_THRESHOLD = 0.5

def model(dbt, session):
    # tables = dbt.ref("stg_pii__get_schema_tables")
    schema_name = 'COFFEE_SHOP'
    tables_query = """
        select table_schema,
            table_name,
            table_type,
            row_count
        from information_schema.tables
        where table_type = 'BASE TABLE'
            and row_count is not null
            and row_count > 0
            and table_name not like 'FIVETRAN%'
            and table_schema = '{}'
        order by 1,2
    """.format(schema_name)
    tables = session.sql(tables_query)
  
    # Convert to pandas dataframe and reset index
    tables_df = tables.to_pandas()
    tables_df = tables_df.reset_index()

    # Table query to Extract Semantic Categories (Snowflake function) for table
    table_pii_query = """
        with pii1 as (
        select
            t.key::varchar as column_name,
            t.value:"privacy_category"::varchar as privacy_category,  
            t.value:"semantic_category"::varchar as semantic_category,
            t.value:"extra_info":"probability"::number(10,2) as probability,
            t.value:"extra_info":"alternates"::variant as alternates
        from table(flatten(extract_semantic_categories('[TABLE_SCHEMA].[TABLE_NAME]')::variant)) as t
        ),
        pii2 as (
        select
            p.column_name,
            a.index,
            a.value:"privacy_category"::varchar as privacy_category,  
            a.value:"semantic_category"::varchar as semantic_category,
            a.value:"probability"::number(10,2) as probability,
            row_number() over (partition by p.column_name order by probability desc) as row_num
        from pii1 p,
        lateral flatten(input=>p.alternates) a
        )
        select
            coalesce(p2.column_name, p1.column_name) as column_name,
            coalesce(p2.privacy_category, p1.privacy_category) as privacy_category,  
            coalesce(p2.semantic_category, p1.semantic_category) as semantic_category,
            coalesce(p2.probability, p1.probability) as probability
        from pii1 as p1
        left join pii2 as p2
            on p1.column_name = p2.column_name
            and p2.row_num = 1
    """

    table_tag_query = """
        alter table [TABLE_SCHEMA].[TABLE_NAME] set tag table_pii_flag = 'Yes'
    """

    column_tag_query = """
        alter table [TABLE_SCHEMA].[TABLE_NAME] modify column [COLUMN_NAME] set tag column_pii_type_snowflake = '[SEMANTIC_CATEGORY]'
    """

    # Initialize results_df
    results_df = pd.DataFrame()

    # Loop over table rows in tables_df (one row per table in schema)
    # t: table index
    for t, table in tables_df.iterrows():
        # Get values from table dataframe row
        table_schema = table['TABLE_SCHEMA']
        table_name = table['TABLE_NAME']
        table_type = table['TABLE_TYPE']
        row_count = table['ROW_COUNT']
        
        # Initialize table_has_pii
        table_has_pii = False

        # Inject table_schema and table_name into pii_query
        this_column_pii_query = table_pii_query.replace('[TABLE_SCHEMA]', table_schema).replace('[TABLE_NAME]', table_name)
        
        # Run this_column_pii_query and convert to dataframe
        column_pii_results = session.sql(this_column_pii_query)
        column_pii_df = column_pii_results.to_pandas()

        for c, column in column_pii_df.iterrows():
            # Get values from column dataframe row
            column_name = column['COLUMN_NAME']
            privacy_category = column['PRIVACY_CATEGORY']
            semantic_category = column['SEMANTIC_CATEGORY']
            probability = column['PROBABILITY']

            if privacy_category is not None:
                if probability >= PROBABILITY_THRESHOLD:
                    table_has_pii = True
                    # Tag column_pii_type_snowflake = semantic_category for PII Columns
                    this_column_tag_query = column_tag_query.replace('[TABLE_SCHEMA]', table_schema).replace('[TABLE_NAME]', table_name).replace('[COLUMN_NAME]', column_name).replace('[SEMANTIC_CATEGORY]', semantic_category)
                    column_tag_results = session.sql(this_column_tag_query).collect()

                    # Get UTC timestamp for Now
                    utc_timestamp = datetime.datetime.now(timezone.utc).replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    # Create new_row for appending to results_df
                    new_row = {
                        'TABLE_SCHEMA': table_schema,
                        'TABLE_NAME': table_name,
                        'TABLE_TYPE': table_type,
                        'ROW_COUNT': row_count,
                        'COLUMN_NAME': column_name,
                        'COLUMN_PII_TYPE': semantic_category,
                        'MODIFIED_TS': utc_timestamp
                    }

                    results_df = results_df.append(new_row, ignore_index=True)

        # Tag table_pii_flag = 'Yes' for tables with PII Columns
        this_table_tag_query = table_tag_query.replace('[TABLE_SCHEMA]', table_schema).replace('[TABLE_NAME]', table_name)
        if table_has_pii:
            table_tag_results = session.sql(this_table_tag_query).collect()

    return results_df