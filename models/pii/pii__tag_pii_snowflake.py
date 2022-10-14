from datetime import timezone
import datetime
import pandas
import pandas as pd
from pandas import DataFrame


def model(dbt, session):
    # Get variables (from _pii__models.yml and dbt_project.yml)
    schema_name = dbt.config.get('pii_schema')
    db_name = dbt.config.get('pii_database')
    exclude_tables = dbt.config.get('pii_exclude_tables')
    # Probability (Confidence) Threhold for tagging PII Columns
    probability_threshold = float(dbt.config.get('pii_probability_threshold'))
    # Rows to scan to evaluate semantic category (PII type)
    rows_to_scan = int(dbt.config.get('pii_rows_to_scan'))

    # Query info schema to get table metadata for tables in db_name.schema_name
    tables_query = """
        select 
            table_catalog,
            table_schema,
            table_name,
            table_type,
            row_count
        from {}.information_schema.tables
        where table_type = 'BASE TABLE'
            and row_count is not null
            and row_count > 0
            and table_name not in {}
            and table_schema = '{}'
            and table_catalog = '{}'
        order by 1,2
    """.format(db_name, exclude_tables, schema_name, db_name)
    tables = session.sql(tables_query)
  
    # Convert to pandas dataframe and reset index
    tables_df = tables.to_pandas()
    tables_df = tables_df.reset_index()

    # Table query to Extract Semantic Categories (Snowflake function) for table
    # Reference: https://docs.snowflake.com/en/user-guide/governance-classify-concepts.html#semantic-category-probabilities-and-alternates
    table_pii_query = """
        with col_md as (
        select
            table_schema,
            table_name,
            column_name,
            data_type
        from [TABLE_CATALOG].information_schema.columns
        where table_schema = '[TABLE_SCHEMA]'
            and table_name = '[TABLE_NAME]'
        ),
        pii1 as (
        select
            t.key::varchar as column_name,
            t.value:"privacy_category"::varchar as privacy_category,  
            t.value:"semantic_category"::varchar as semantic_category,
            t.value:"extra_info":"probability"::number(10,2) as probability,
            t.value:"extra_info":"alternates"::variant as alternates
        from table(flatten(extract_semantic_categories('[TABLE_CATALOG].[TABLE_SCHEMA].[TABLE_NAME]', [ROWS_TO_SCAN])::variant)) as t
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
            cm.column_name,
            cm.data_type,
            coalesce(p2.privacy_category, p1.privacy_category) as privacy_category,  
            coalesce(p2.semantic_category, p1.semantic_category) as semantic_category,
            coalesce(p2.probability, p1.probability) as probability
        from col_md as cm
        left join pii1 as p1
            on cm.column_name = p1.column_name
        left join pii2 as p2
            on cm.column_name = p2.column_name
            and p2.row_num = 1
        where coalesce(p2.privacy_category, p1.privacy_category) is not null
    """

    table_tag_query = """
        alter table [TABLE_CATALOG].[TABLE_SCHEMA].[TABLE_NAME] set tag table_pii_flag = 'Yes'
    """

    column_tag_query = """
        alter table [TABLE_CATALOG].[TABLE_SCHEMA].[TABLE_NAME] modify column [COLUMN_NAME] set tag column_pii_type_snowflake = '[SEMANTIC_CATEGORY]'
    """

    # Initialize results_df
    results_df = pd.DataFrame()

    # Loop over table rows in tables_df (one row per table in schema)
    # t: table index
    for t, table in tables_df.iterrows():
        # Get values from table dataframe row
        table_catalog = table['TABLE_CATALOG']
        table_schema = table['TABLE_SCHEMA']
        table_name = table['TABLE_NAME']
        table_type = table['TABLE_TYPE']
        row_count = table['ROW_COUNT']
        
        # Initialize table_has_pii
        table_has_pii = False

        # Inject table_schema and table_name into pii_query
        this_column_pii_query = table_pii_query.replace('[TABLE_CATALOG]', table_catalog).replace('[TABLE_SCHEMA]', table_schema).replace('[TABLE_NAME]', table_name).replace('[ROWS_TO_SCAN]', str(rows_to_scan))
        
        # Run this_column_pii_query and convert to dataframe
        column_pii_results = session.sql(this_column_pii_query)
        column_pii_df = column_pii_results.to_pandas()

        for c, column in column_pii_df.iterrows():
            # Get values from column dataframe row
            column_name = column['COLUMN_NAME']
            data_type = column['DATA_TYPE']
            privacy_category = column['PRIVACY_CATEGORY']
            semantic_category = column['SEMANTIC_CATEGORY']
            probability = column['PROBABILITY']

            # Ignore postal code type assigned to numerical fields
            if semantic_category.upper() == 'US_POSTAL_CODE':
                if data_type != 'TEXT':
                    privacy_category = None

            if privacy_category is not None:
                if probability >= probability_threshold:
                    table_has_pii = True
                    # Tag column_pii_type_snowflake = semantic_category for PII Columns
                    this_column_tag_query = column_tag_query.replace('[TABLE_CATALOG]', table_catalog).replace('[TABLE_SCHEMA]', table_schema).replace('[TABLE_NAME]', table_name).replace('[COLUMN_NAME]', column_name).replace('[SEMANTIC_CATEGORY]', semantic_category)
                    column_tag_results = session.sql(this_column_tag_query).collect()

                    # Get UTC timestamp for Now
                    utc_timestamp = datetime.datetime.now(timezone.utc).replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    # Adjust names for first and last names
                    if semantic_category.upper() == 'NAME':
                        if "FIRST" in column_name.upper():
                            semantic_category = "FIRST_NAME"
                        elif "LAST" in column_name.upper():
                            semantic_category = "LAST_NAME"
                        else:
                            semantic_category = "NAME"

                    # Create new_row for appending to results_df
                    new_row = {
                        'TABLE_CATALOG': table_catalog,
                        'TABLE_SCHEMA': table_schema,
                        'TABLE_NAME': table_name,
                        'TABLE_TYPE': table_type,
                        'ROW_COUNT': row_count,
                        'COLUMN_NAME': column_name,
                        'DATA_TYPE': data_type,
                        'COLUMN_PII_TYPE': semantic_category,
                        'PROBABILITY': probability,
                        'MODIFIED_TS': utc_timestamp
                    }

                    new_row = pd.DataFrame([new_row])
                    results_df = pd.concat([results_df, new_row], ignore_index=True)

        # Tag table_pii_flag = 'Yes' for tables with PII Columns
        this_table_tag_query = table_tag_query.replace('[TABLE_CATALOG]', table_catalog).replace('[TABLE_SCHEMA]', table_schema).replace('[TABLE_NAME]', table_name)
        if table_has_pii:
            table_tag_results = session.sql(this_table_tag_query).collect()

    return results_df