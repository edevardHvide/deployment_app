def generate_control_table_backup_sql(table_suffix):
    """Generate SQL for backing up control tables"""
    return f"""-- Make a copy of DWH.CONTROL_TABLE_STAGE
WITH cte AS ( 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('ST_Full_Initial') 	
    UNION ALL 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('ST_Full_Daily') 	
) 	
SELECT *	
INTO sandbox.temp_control_table_st_{table_suffix} FROM cte;

-- Make a copy of DWH.CONTROL_TABLE_HS
SELECT TOP 1 * 
INTO sandbox.temp_control_table_hs_{table_suffix} 
FROM DWH.CONTROL_TABLE_HS;

-- Make a copy of DWH.JOB_CONTROL
SELECT * 
INTO sandbox.temp_control_table_job_{table_suffix} 
FROM DWH.JOB_CONTROL 
WHERE job_name IN ('ST_Full_Daily','ST_Full_Initial','HS_Full_Daily','HS_Full_Daily_Control','ST_Placeholder');
"""

def generate_st_control_table_sql(table_suffix, source_system_initial, source_system_daily,
                                src_schema_name, src_table_name, src_table_name_ct,
                                tgt_schema_name_st, tgt_table_name_st, business_key,
                                incremental_filter_st, incremental_filter_timezone,
                                delete_type, src_delete_column, src_delete_value):
    """Generate SQL for ST control table updates"""
    delete_type_sql = "NULL" if delete_type is None else f"'{delete_type}'"
    src_delete_column_sql = "NULL" if src_delete_column is None else f"'{src_delete_column}'"
    src_delete_value_sql = "NULL" if src_delete_value is None else f"'{src_delete_value}'"
    
    # Special job name for Profisee source
    initial_job_name = "ST_profisee_initial" if "Profisee" in source_system_initial else "ST_Full_Initial"
    daily_job_name = "ST_profisee_daily" if "Profisee" in source_system_daily else "ST_Full_Daily"
    
    return f"""-- Update temporary control table for stage to reflect Initial Load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = '{initial_job_name}',
    source_system = '{source_system_initial}',
    src_schema_name = '{src_schema_name}',
    src_table_name = '{src_table_name}',
    tgt_schema_name = '{tgt_schema_name_st}',
    tgt_table_name = '{tgt_table_name_st}',
    business_key = '{business_key}',
    initial_load_valid_from_column = '__lowDate',
    incremental_filter_column = '{incremental_filter_st}',
    incremental_filter_column_timezone = '{incremental_filter_timezone}',
    skip = 0,
    priority = 0,
    delete_type = {delete_type_sql},
    src_delete_column = {src_delete_column_sql},
    src_delete_value = {src_delete_value_sql}
WHERE job_name = 'ST_Full_Initial';

-- Update temporary control table for stage to reflect Daily load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = '{daily_job_name}',
    source_system = '{source_system_daily}',
    src_schema_name = '{src_schema_name}',
    src_table_name = '{src_table_name_ct}',
    tgt_schema_name = '{tgt_schema_name_st}',
    tgt_table_name = '{tgt_table_name_st}',
    business_key = '{business_key}',
    initial_load_valid_from_column = '__lowDate',
    incremental_filter_column = '{incremental_filter_st}',
    incremental_filter_column_timezone = '{incremental_filter_timezone}',
    skip = 0,
    priority = 0,
    delete_type = {delete_type_sql},
    src_delete_column = {src_delete_column_sql},
    src_delete_value = {src_delete_value_sql}
WHERE job_name = 'ST_Full_Daily';
"""

def generate_hs_control_table_sql(table_suffix, tgt_schema_name_st, tgt_table_name_st,
                                tgt_schema_name_hs, tgt_table_name_hs, business_key,
                                primary_key, incremental_filter_hs, incremental_filter_timezone,
                                scd_type, scd2_columns, prescript, postscript, partitions,
                                use_source_column_for_valid_dates, source_column_for_valid_from_date,
                                source_column_for_sorting, source_system_initial=None):
    """Generate SQL for HS control table updates"""
    prescript_sql = "''" if not prescript else f"'{prescript}'"
    postscript_sql = "''" if not postscript else f"'{postscript}'"
    
    # Only use these values when scd_type is "SCD2 from CT"
    if scd_type == "SCD2 from CT":
        use_source_column_value = 1 if use_source_column_for_valid_dates else 0
        source_column_sql = f"'{source_column_for_valid_from_date}'" if use_source_column_for_valid_dates else "NULL"
    else:
        use_source_column_value = 0
        source_column_sql = "NULL"
    
    sorting_column_clause = ""
    if scd_type == "SCD2 from CT" and source_column_for_sorting:
        sorting_column_clause = f"""
    source_column_for_sorting = '{source_column_for_sorting}',"""
    
    # Special job name for Profisee source
    job_name = "HS_Profisee_Daily" if source_system_initial and "Profisee" in source_system_initial else "HS_Full_Daily"
    
    return f"""-- Update temporary control table for historic stage to reflect daily load values
UPDATE sandbox.temp_control_table_hs_{table_suffix}
SET job_name = '{job_name}',
    src_schema_name = '{tgt_schema_name_st}',
    src_table_name = '{tgt_table_name_st}', 
    tgt_schema_name = '{tgt_schema_name_hs}',
    tgt_table_name = '{tgt_table_name_hs}', 
    business_key = '{business_key}',
    primary_key = '{primary_key}',
    incremental_filter_column = '{incremental_filter_hs}',
    incremental_filter_column_timezone = '{incremental_filter_timezone}',
    scd_type = '{scd_type}',
    scd2_columns = '{scd2_columns}',
    skip = 0,
    priority = 0,
    prescript = {prescript_sql},
    postscript = {postscript_sql},
    partitions = {partitions},
    use_source_column_for_valid_dates = {use_source_column_value},
    source_column_for_valid_from_date = {source_column_sql}{sorting_column_clause}
WHERE job_name = 'HS_Full_Daily';
"""

def generate_job_control_sql(table_suffix):
    """Generate SQL for job control table updates"""
    return f"""-- Update the control table so that the jobs are set to STATUS='SUCCESS'
UPDATE sandbox.temp_control_table_job_{table_suffix}
SET 
    STATUS = 'SUCCESS',
    LAST_LOAD_DATE = '1970-01-01',
    JOB_INTERVAL_IN_MINUTES = 0
WHERE job_name IN ('HS_Full_Daily','ST_Full_Daily','HS_Full_Daily_Control','ST_Full_Initial');
"""

def generate_hs_table_sql(tgt_schema_name_hs, tgt_table_name_hs, tgt_schema_name_st=None, tgt_table_name_st=None):
    """Generate SQL for HS table creation"""
    if not tgt_schema_name_st or not tgt_table_name_st:
        return f"""-- Error: Source table information is missing.
-- Please provide the ST schema and table name to create the HS table correctly."""
    
    return f"""-- Create the HS table with technical columns
SELECT * INTO {tgt_schema_name_hs}.{tgt_table_name_hs} FROM {tgt_schema_name_st}.{tgt_table_name_st} WHERE 1 = 0;

ALTER TABLE {tgt_schema_name_hs}.{tgt_table_name_hs}
ADD TC_CURRENT_FLAG VARCHAR(1), 
    TC_VALID_FROM_DATE DATETIME2(0), 
    TC_VALID_TO_DATE DATETIME2(0), 
    TC_CHECKSUM_BUSKEY VARCHAR(32), 
    TC_CHECKSUM_SCD VARCHAR(32), 
    TC_DELETED_FLAG VARCHAR(1), 
    TC_DELETED_DATETIME DATETIME2(0),
    TC_INSERTED_DATE DATETIME2(0),
    TC_ROW_ID BIGINT IDENTITY(1,1) PRIMARY KEY;

ALTER TABLE {tgt_schema_name_hs}.{tgt_table_name_hs}
DROP COLUMN TC_INITIAL_LOAD_VALID_FROM_DATE;
"""

def generate_helper_table_sql(create_helper_table, helper_schema, business_key_column, src_table_name):
    """Generate SQL for helper table creation"""
    if not create_helper_table or not business_key_column:
        return None
    
    base_table_name = src_table_name
    if "_" in src_table_name:
        base_table_name = src_table_name.split("_", 1)[1] if src_table_name.count("_") > 0 else src_table_name
    
    helper_table_name = f"HLP_BK_{base_table_name}"
    identity_column_name = f"BK_{base_table_name}"
    
    return f"""SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [{helper_schema}].[{helper_table_name}](
    [{identity_column_name}] [int] IDENTITY(1,1) NOT NULL,
    [{business_key_column}] [bigint] NOT NULL,
PRIMARY KEY CLUSTERED 
(
    [{identity_column_name}] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
"""

def generate_main_table_sql(create_main_table, main_table_schema,
                          main_table_columns, src_table_name, main_table_name=None):
    """Generate SQL for main table creation"""
    if not create_main_table:
        return None
    
    # If main_table_name is not provided, derive it from src_table_name (for backward compatibility)
    if not main_table_name:
        base_table_name = src_table_name
        if "_" in src_table_name:
            base_table_name = src_table_name.split("_", 1)[1] if src_table_name.count("_") > 0 else src_table_name
        
        main_table_name = f"DIM_{base_table_name}"
    
    # Derive column names based on the table name
    base_table_name = main_table_name.replace("DIM_", "")
    pk_column_name = f"PK_{main_table_name}"
    bk_column_name = f"BK_{base_table_name}"
    
    column_defs = []
    for line in main_table_columns.strip().split('\n'):
        if line.strip():
            column_defs.append(f"\t{line.strip()}")
    
    return f"""SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [{main_table_schema}].[{main_table_name}](
\t[{pk_column_name}] [int] IDENTITY(1,1) NOT NULL,
\t[{bk_column_name}] [int] NULL,
{chr(10).join(column_defs)},
\t[TC_CURRENT_FLAG] [varchar](1) NULL,
\t[TC_VALID_FROM_DATE] [datetime2](0) NULL,
\t[TC_VALID_TO_DATE] [datetime2](0) NULL,
\t[TC_CHECKSUM_SCD] [varchar](32) NULL,
\t[TC_CHECKSUM_BUSKEY] [varchar](32) NULL,
\t[TC_DELETED_FLAG] [varchar](1) NULL,
\t[TC_DELETED_DATETIME] [datetime2](0) NULL,
\t[TC_SOURCE_SYSTEM] [varchar](10) NULL,
\t[TC_UPDATED_DATE] [datetime2](0) NULL,
\t[TC_ROW_ID] [int] NULL,
PRIMARY KEY CLUSTERED 
(
\t[{pk_column_name}] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
""" 