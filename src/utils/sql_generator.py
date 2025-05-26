def generate_control_table_backup_sql(table_suffix, source_system_initial=None, source_system_daily=None):
    """Generate SQL for backing up control tables"""
    # Determine the correct job names based on source system
    st_initial_job = "ST_Profisee_Initial" if source_system_initial and "Profisee_dev" in source_system_initial else "ST_Full_Initial"
    st_daily_job = "ST_Profisee_Daily" if source_system_daily and "Profisee_dev" in source_system_daily else "ST_Full_Daily"
    hs_daily_job = "HS_Profisee_Daily" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily"
    hs_control_job = "HS_Profisee_Daily_Control" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily_Control"
    
    # Create a list of all job names that need to be included in the backup
    job_list = f"'{st_daily_job}','{st_initial_job}','{hs_daily_job}','{hs_control_job}','ST_Placeholder'"
    
    return f"""-- Make a copy of DWH.CONTROL_TABLE_STAGE
WITH cte AS ( 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('{st_initial_job}') 	
    UNION ALL 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('{st_daily_job}') 	
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
WHERE job_name IN ({job_list});
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
    initial_job_name = "ST_Profisee_Initial" if source_system_initial and "Profisee_dev" in source_system_initial else "ST_Full_Initial"
    daily_job_name = "ST_Profisee_Daily" if source_system_daily and "Profisee_dev" in source_system_daily else "ST_Full_Daily"
    
    # Default job names before updates - for WHERE clause when querying specific backup tables
    # But in other cases we want the same job name in WHERE as in SET
    initial_job_name_where = initial_job_name
    daily_job_name_where = daily_job_name
    
    # Handle table names for Profisee sources
    if source_system_initial and "Profisee_dev" in source_system_initial:
        # For Profisee, use ST_PRO_[source_table_name] pattern
        # Preserve the original case of the source table name exactly
        actual_tgt_table_name_st = f"ST_PRO_{src_table_name}"
    else:
        # For other sources, use the provided target table name
        actual_tgt_table_name_st = tgt_table_name_st
    
    return f"""-- Update temporary control table for stage to reflect Initial Load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = '{initial_job_name}',
    source_system = '{source_system_initial}',
    src_schema_name = '{src_schema_name}',
    src_table_name = '{src_table_name}',
    tgt_schema_name = '{tgt_schema_name_st}',
    tgt_table_name = '{actual_tgt_table_name_st}',
    business_key = '{business_key}',
    initial_load_valid_from_column = '__lowDate',
    incremental_filter_column = '{incremental_filter_st}',
    incremental_filter_column_timezone = '{incremental_filter_timezone}',
    skip = 0,
    priority = 0,
    delete_type = {delete_type_sql},
    src_delete_column = {src_delete_column_sql},
    src_delete_value = {src_delete_value_sql}
WHERE job_name = '{initial_job_name_where}';

-- Update temporary control table for stage to reflect Daily load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = '{daily_job_name}',
    source_system = '{source_system_daily}',
    src_schema_name = '{src_schema_name}',
    src_table_name = '{src_table_name_ct}',
    tgt_schema_name = '{tgt_schema_name_st}',
    tgt_table_name = '{actual_tgt_table_name_st}',
    business_key = '{business_key}',
    initial_load_valid_from_column = '__lowDate',
    incremental_filter_column = '{incremental_filter_st}',
    incremental_filter_column_timezone = '{incremental_filter_timezone}',
    skip = 0,
    priority = 0,
    delete_type = {delete_type_sql},
    src_delete_column = {src_delete_column_sql},
    src_delete_value = {src_delete_value_sql}
WHERE job_name = '{daily_job_name_where}';
"""

def generate_hs_control_table_sql(table_suffix, source_system_initial, source_system_daily,
                                src_schema_name, src_table_name,
                                tgt_schema_name_st, tgt_schema_name_hs, tgt_table_name_hs,
                                business_key, primary_key="TC_ROW_ID", incremental_filter_hs="__fullLoad", 
                                incremental_filter_timezone="UTC", scd_type="SCD2", scd2_columns="__allColumns",
                                prescript="", postscript="", partitions=1, 
                                use_source_column_for_valid_dates=False, source_column_for_valid_from_date=None,
                                source_column_for_sorting=None):
    """Generate SQL for HS control table updates"""
    # Use empty string for prescript/postscript if they're None
    prescript_sql = "''" if not prescript else f"'{prescript}'"
    postscript_sql = "''" if not postscript else f"'{postscript}'"
    
    # Handle use_source_column values
    use_source_column_value = 1 if use_source_column_for_valid_dates else 0
    source_column_sql = f"'{source_column_for_valid_from_date}'" if source_column_for_valid_from_date else "NULL"
    
    # Handle sorting column if provided
    sorting_column_clause = ""
    if source_column_for_sorting:
        sorting_column_clause = f"\n    source_column_for_sorting = '{source_column_for_sorting}',"
    
    # Special job name for Profisee source
    hs_job_name = "HS_Profisee_Daily" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily"
    
    # Handle table names for Profisee sources
    if source_system_initial and "Profisee_dev" in source_system_initial:
        # For Profisee, use ST_PRO_[source_table_name] for source and HS_PRO_[source_table_name] for target
        # Preserve the exact source table name, just add prefixes
        actual_src_table_name = f"ST_PRO_{src_table_name}"
        actual_tgt_table_name_hs = f"HS_PRO_{src_table_name}"
    else:
        # For other sources, use the provided target table names
        actual_src_table_name = f"ST_{src_table_name}" if tgt_schema_name_st == "ST" else src_table_name
        actual_tgt_table_name_hs = tgt_table_name_hs
    
    return f"""-- Update temporary control table for historic stage to reflect daily load values
UPDATE sandbox.temp_control_table_hs_{table_suffix}
SET job_name = '{hs_job_name}',
    src_schema_name = '{tgt_schema_name_st}',
    src_table_name = '{actual_src_table_name}', 
    tgt_schema_name = '{tgt_schema_name_hs}',
    tgt_table_name = '{actual_tgt_table_name_hs}', 
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
;
"""

def generate_job_control_sql(table_suffix, source_system_initial=None, source_system_daily=None):
    """Generate SQL for job control table updates"""
    # Determine the correct job names based on source system
    st_initial_job = "ST_Profisee_Initial" if source_system_initial and "Profisee_dev" in source_system_initial else "ST_Full_Initial"
    st_daily_job = "ST_Profisee_Daily" if source_system_daily and "Profisee_dev" in source_system_daily else "ST_Full_Daily"
    hs_daily_job = "HS_Profisee_Daily" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily"
    hs_control_job = "HS_Profisee_Daily_Control" if source_system_initial and "Profisee_dev" in source_system_initial else "HS_Full_Daily_Control"
    
    # Create the list of job names to include in the SQL
    job_list = f"'{hs_daily_job}','{st_daily_job}','{hs_control_job}','{st_initial_job}','ST_Placeholder'"
    
    return f"""-- Update the control table so that the jobs are set to STATUS='SUCCESS'
UPDATE sandbox.temp_control_table_job_{table_suffix}
SET 
    STATUS = 'SUCCESS',
    LAST_LOAD_DATE = '1970-01-01',
    JOB_INTERVAL_IN_MINUTES = 0
WHERE job_name IN ({job_list});
"""

def generate_hs_table_sql(tgt_schema_name_hs, tgt_table_name_hs, tgt_schema_name_st=None, tgt_table_name_st=None, source_system=None, src_table_name=None):
    """Generate SQL for HS table creation"""
    if not tgt_schema_name_st or not tgt_table_name_st:
        # If we have source_system and src_table_name, we can try to construct the ST table name
        if source_system and "Profisee_dev" in source_system and src_table_name:
            # Preserve the exact source table name, just add prefixes
            tgt_table_name_st = f"ST_PRO_{src_table_name}"
            # Also update the HS table name to match the source table name exactly
            tgt_table_name_hs = f"HS_PRO_{src_table_name}"
        else:
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

-- Drop any initial load column that's no longer needed
IF COL_LENGTH('{tgt_schema_name_hs}.{tgt_table_name_hs}', 'TC_INITIAL_LOAD_VALID_FROM_DATE') IS NOT NULL
BEGIN
    ALTER TABLE {tgt_schema_name_hs}.{tgt_table_name_hs}
    DROP COLUMN TC_INITIAL_LOAD_VALID_FROM_DATE;
END
"""

def generate_helper_table_sql(create_helper_table, helper_schema, business_key_column, src_table_name, business_key=""):
    """Generate SQL for helper table creation"""
    if not create_helper_table:
        return None
    
    # Extract base table name and convert to uppercase for table naming
    base_table_name = src_table_name
    if "_" in src_table_name:
        base_table_name = src_table_name.split("_", 1)[1] if src_table_name.count("_") > 0 else src_table_name
    
    # Convert base table name to uppercase for both table and column names
    base_table_name_upper = base_table_name.upper()
    
    # Create uppercase helper table and dimension table names
    helper_table_name = f"HLP_BK_{base_table_name_upper}"
    dim_table_name = f"DIM_{base_table_name_upper}"
    
    # First column name is BK_DIM_TABLENAME
    identity_column_name = f"BK_{dim_table_name}"
    
    # For the second column, use the business key
    # If business_key contains multiple comma-separated keys, use the first one
    business_key_column_name = business_key.split(",")[0].strip() if business_key else business_key_column
    
    return f"""SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [{helper_schema}].[HLP_BK_{dim_table_name}](
    [{identity_column_name}] [int] IDENTITY(1,1) NOT NULL,
    [{business_key_column_name}] [bigint] NOT NULL,
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
    
    # Extract base table name and convert to uppercase
    base_table_name = src_table_name
    if "_" in src_table_name:
        base_table_name = src_table_name.split("_", 1)[1] if src_table_name.count("_") > 0 else src_table_name
    
    # Convert base table name to uppercase for dimension table naming
    base_table_name_upper = base_table_name.upper()
    
    # If main_table_name is not provided, derive it from src_table_name with uppercase
    if not main_table_name:
        main_table_name = f"DIM_{base_table_name_upper}"
    else:
        # If provided, ensure it's still uppercase
        if main_table_name.startswith("DIM_"):
            table_part = main_table_name[4:]  # Remove the DIM_ prefix
            main_table_name = f"DIM_{table_part.upper()}"
        else:
            main_table_name = main_table_name.upper()
    
    # Derive column names based on the table name - all in uppercase
    pk_column_name = f"PK_{main_table_name}"
    bk_column_name = f"BK_{base_table_name_upper}"
    
    column_defs = []
    for line in main_table_columns.strip().split('\n'):
        if line.strip():
            column_defs.append(f"\t{line.strip()}")
    
    # The job control table entry name should match the DIM table name
    job_control_entry = f"""
-- Add the dimension table job to DWH.JOB_CONTROL
INSERT INTO DWH.JOB_CONTROL VALUES 
('{main_table_name}','1970-01-01 00:00:00','1970-01-01 00:00:00','SUCCESS','1970-01-01 00:00:00',0,NULL)
GO
"""
    
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
GO{job_control_entry}
"""

def generate_hs_table_quick_creation_sql(tgt_schema_name_hs, tgt_table_name_hs, tgt_schema_name_st, tgt_table_name_st=None, source_system=None, src_table_name=None):
    """Generate SQL script for quick HS table creation after ST job has run with invalid HS job name"""
    # If tgt_table_name_st is missing but we have source_system and src_table_name, construct the name
    if not tgt_table_name_st and source_system and "Profisee_dev" in source_system and src_table_name:
        # Preserve the exact source table name, just add prefixes
        tgt_table_name_st = f"ST_PRO_{src_table_name}"
        # Also update the HS table name to match the source table name exactly
        tgt_table_name_hs = f"HS_PRO_{src_table_name}"
    
    return f"""-- Quick HS table creation script
-- Run this after the ST job has completed with invalid HS job name

-- Create the HS table structure from the ST table
SELECT * INTO {tgt_schema_name_hs}.{tgt_table_name_hs} FROM {tgt_schema_name_st}.{tgt_table_name_st} WHERE 1 = 0;

-- Add all necessary technical columns
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

-- Drop any initial load column that's no longer needed
IF COL_LENGTH('{tgt_schema_name_hs}.{tgt_table_name_hs}', 'TC_INITIAL_LOAD_VALID_FROM_DATE') IS NOT NULL
BEGIN
    ALTER TABLE {tgt_schema_name_hs}.{tgt_table_name_hs}
    DROP COLUMN TC_INITIAL_LOAD_VALID_FROM_DATE;
END

-- After running this script, you can either:
-- 1. Run the Stage job again with correct parameters to do the full initial load, or
-- 2. Use ST_Placeholder as the job name to only run the HS part
""" 