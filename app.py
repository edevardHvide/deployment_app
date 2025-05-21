import streamlit as st
import pandas as pd
from datetime import datetime
import io
import json

# Suppress warnings that might appear in Databricks environment
import warnings
warnings.filterwarnings("ignore")

def export_parameters():
    """Export all user input parameters to a JSON file"""
    params = {
        "user_initials": user_initials,
        "source_system_initial": source_system_initial,
        "source_system_daily": source_system_daily,
        "src_schema_name": src_schema_name,
        "src_table_name": src_table_name,
        "tgt_schema_name_st": tgt_schema_name_st,
        "tgt_table_name_st": tgt_table_name_st,
        "tgt_schema_name_hs": tgt_schema_name_hs,
        "tgt_table_name_hs": tgt_table_name_hs,
        "business_key": business_key,
        "primary_key": primary_key,
        "incremental_filter_st": incremental_filter_st,
        "incremental_filter_hs": incremental_filter_hs,
        "incremental_filter_timezone": incremental_filter_timezone,
        "scd_type": scd_type,
        "scd2_columns_option": scd2_columns_option,
        "scd2_columns": scd2_columns if scd2_columns_option == "Specify Columns" else "__allColumns",
        "create_helper_table": create_helper_table,
        "helper_schema": helper_schema if create_helper_table else "",
        "business_key_column": business_key_column if create_helper_table else "",
        "delete_type": delete_type,
        "src_delete_column": src_delete_column if delete_type == "SOFT" else None,
        "src_delete_value": src_delete_value if delete_type == "SOFT" else None,
        "prescript": prescript,
        "postscript": postscript,
        "partitions": partitions,
        "use_source_column_for_valid_dates": use_source_column_for_valid_dates,
        "source_column_for_valid_from_date": source_column_for_valid_from_date if use_source_column_for_valid_dates else "",
        "source_column_for_sorting": source_column_for_sorting if scd_type == "SCD2 from CT" else "",
        "create_main_table": create_main_table,
        "main_table_schema": main_table_schema if create_main_table else "",
        "main_table_prefix": main_table_prefix if create_main_table else "",
        "main_table_columns": main_table_columns if create_main_table else "",
        "skip_st_table": skip_st_table,
        "skip_hs_table": skip_hs_table,
        "skip_main_table": skip_main_table
    }
    return json.dumps(params, indent=4)

def import_parameters(json_str):
    """Import parameters from a JSON string and return a dictionary of values"""
    try:
        params = json.loads(json_str)
        return params
    except json.JSONDecodeError:
        st.error("Invalid JSON format")
        return None

st.set_page_config(page_title="DWH Table Deployment Helper", layout="wide")

st.title("Data Warehouse Table Deployment Helper")

# Initialize session state for SQL generation
if 'sql_generated' not in st.session_state:
    st.session_state.sql_generated = False
    st.session_state.all_sql = ""
    st.session_state.timestamp = ""
    st.session_state.table_suffix = ""

with st.sidebar:
    st.header("Table Information")
    
    # Add import/export section at the top of sidebar
    with st.expander("Import/Export Parameters"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Export Parameters")
            if st.button("Export Current Parameters"):
                # Get all current parameter values
                current_params = {
                    "user_initials": st.session_state.get("user_initials", ""),
                    "source_system_initial": st.session_state.get("source_system_initial", "Replicate_Full"),
                    "source_system_daily": st.session_state.get("source_system_daily", "Replicate_CDC"),
                    "src_schema_name": st.session_state.get("src_schema_name", "TIA"),
                    "src_table_name": st.session_state.get("src_table_name", ""),
                    "tgt_schema_name_st": st.session_state.get("tgt_schema_name_st", "ST"),
                    "tgt_table_name_st": st.session_state.get("tgt_table_name_st", ""),
                    "tgt_schema_name_hs": st.session_state.get("tgt_schema_name_hs", "HS"),
                    "tgt_table_name_hs": st.session_state.get("tgt_table_name_hs", ""),
                    "business_key": st.session_state.get("business_key", ""),
                    "primary_key": st.session_state.get("primary_key", "TC_ROW_ID"),
                    "incremental_filter_st": st.session_state.get("incremental_filter_st", "__fullLoad"),
                    "incremental_filter_hs": st.session_state.get("incremental_filter_hs", "__fullLoad"),
                    "incremental_filter_timezone": st.session_state.get("incremental_filter_timezone", "UTC"),
                    "scd_type": st.session_state.get("scd_type", "SCD2"),
                    "scd2_columns_option": st.session_state.get("scd2_columns_option", "Specify Columns"),
                    "scd2_columns": st.session_state.get("scd2_columns", ""),
                    "create_helper_table": st.session_state.get("create_helper_table", False),
                    "helper_schema": st.session_state.get("helper_schema", "DF"),
                    "business_key_column": st.session_state.get("business_key_column", ""),
                    "delete_type": st.session_state.get("delete_type", None),
                    "src_delete_column": st.session_state.get("src_delete_column", "DELETED_FLAG"),
                    "src_delete_value": st.session_state.get("src_delete_value", "Y"),
                    "prescript": st.session_state.get("prescript", ""),
                    "postscript": st.session_state.get("postscript", ""),
                    "partitions": st.session_state.get("partitions", 1),
                    "use_source_column_for_valid_dates": st.session_state.get("use_source_column_for_valid_dates", True),
                    "source_column_for_valid_from_date": st.session_state.get("source_column_for_valid_from_date", "header__timestamp"),
                    "source_column_for_sorting": st.session_state.get("source_column_for_sorting", "header__change_seq"),
                    "create_main_table": st.session_state.get("create_main_table", False),
                    "main_table_schema": st.session_state.get("main_table_schema", "DIM"),
                    "main_table_prefix": st.session_state.get("main_table_prefix", "DIM_"),
                    "main_table_columns": st.session_state.get("main_table_columns", ""),
                    "skip_st_table": st.session_state.get("skip_st_table", False),
                    "skip_hs_table": st.session_state.get("skip_hs_table", False),
                    "skip_main_table": st.session_state.get("skip_main_table", False)
                }
                params_json = json.dumps(current_params, indent=4)
                st.download_button(
                    label="Download Parameters",
                    data=params_json,
                    file_name=f"dwh_deployment_params_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        with col2:
            st.subheader("Import Parameters")
            uploaded_file = st.file_uploader("Upload Parameters File", type=['json'])
            if uploaded_file is not None:
                params_str = uploaded_file.getvalue().decode()
                try:
                    imported_params = json.loads(params_str)
                    st.success("Parameters loaded successfully! Click 'Apply Parameters' to use them.")
                    if st.button("Apply Parameters"):
                        st.session_state.imported_params = imported_params
                        st.experimental_rerun()
                except json.JSONDecodeError:
                    st.error("Invalid JSON format")
    
    # Check for imported parameters
    if 'imported_params' in st.session_state:
        params = st.session_state.imported_params
        # Clear the imported parameters after using them
        del st.session_state.imported_params
    else:
        params = {}
    
    # User Initials
    user_initials = st.text_input("Your Initials (e.g., skg)", params.get("user_initials", "")).lower()
    st.session_state.user_initials = user_initials
    
    # Source Table Info
    st.subheader("Source Table Configuration")
    
    # Source System Options from the documentation
    source_system_options = [
        "Replicate_Full", 
        "Replicate_CDC", 
        "INS_temporal",
        "TIA",
        "Profisee",
        "Replicate_CDC_AllTransactions",
        "Replicate_CDC_AllTransactions_fromArchive"
    ]
    
    source_system_initial = st.selectbox(
        "Source System (Initial)", 
        source_system_options,
        index=source_system_options.index(params.get("source_system_initial", "Replicate_Full")),
        help="INS_temporal: INS Tables with system version control\nTIA: Reinsurance application\nReplicate_Full: Initial load tables\nReplicate_CDC: CDC tables (latest change per business key)\nProfisee: Profisee production database\nReplicate_CDC_AllTransactions: All transactions from CT table\nReplicate_CDC_AllTransactions_fromArchive: Read from view combining archives"
    )
    st.session_state.source_system_initial = source_system_initial
    
    # Add environment selection for Profisee source system
    profisee_environment_initial = None
    if source_system_initial == "Profisee":
        profisee_environment_initial = st.selectbox(
            "Profisee Environment (Initial)",
            ["prod", "dev", "test"],
            help="prod: Production (no suffix)\ndev: Development (adds _dev suffix)\ntest: Test environment (adds _test suffix)"
        )
        # Apply the suffix to the source system name if not prod
        if profisee_environment_initial != "prod":
            source_system_initial = f"Profisee_{profisee_environment_initial}"
    
    source_system_daily = st.selectbox(
        "Source System (Daily)", 
        source_system_options,
        index=1
    )
    
    # Add environment selection for Profisee source system (daily)
    profisee_environment_daily = None
    if source_system_daily == "Profisee":
        profisee_environment_daily = st.selectbox(
            "Profisee Environment (Daily)",
            ["prod", "dev", "test"],
            help="prod: Production (no suffix)\ndev: Development (adds _dev suffix)\ntest: Test environment (adds _test suffix)"
        )
        # Apply the suffix to the source system name if not prod
        if profisee_environment_daily != "prod":
            source_system_daily = f"Profisee_{profisee_environment_daily}"
    
    src_schema_name = st.text_input("Source Schema Name", params.get("src_schema_name", "TIA"))
    src_table_name = st.text_input("Source Table Name", params.get("src_table_name", ""))
    src_table_name_ct = f"{src_table_name}__ct" if src_table_name and source_system_daily == "Replicate_CDC" else src_table_name
    
    # Target Table Info
    st.subheader("Target Table Configuration")
    tgt_schema_name_st = st.text_input("ST Schema Name", params.get("tgt_schema_name_st", "ST"))
    tgt_table_name_st = st.text_input("ST Table Name", f"ST_{src_table_name}" if src_table_name else params.get("tgt_table_name_st", ""))
    
    tgt_schema_name_hs = st.text_input("HS Schema Name", params.get("tgt_schema_name_hs", "HS"))
    tgt_table_name_hs = st.text_input("HS Table Name", f"HS_{src_table_name}" if src_table_name else params.get("tgt_table_name_hs", ""))
    
    # Business Key
    st.subheader("Key Columns Configuration")
    business_key = st.text_input("Business Key (comma-separated)", params.get("business_key", ""), help="List of columns that identify a unique row in the source")
    primary_key = st.text_input("Primary Key", params.get("primary_key", "TC_ROW_ID"), help="Name of the Primary key field in the target table. Usually an identity column")
    
    # Incremental Load Configuration
    st.subheader("Incremental Load Configuration")
    
    incremental_filter_options = ["__fullLoad", "header__timestamp", "Custom"]
    incremental_filter_st = st.selectbox(
        "Incremental Filter Column (ST)", 
        incremental_filter_options,
        help="__fullLoad: Reads all records from source\nCustom: Specify your own column name",
        index=incremental_filter_options.index(params.get("incremental_filter_st", "__fullLoad"))
    )
    
    if incremental_filter_st == "Custom":
        incremental_filter_st = st.text_input("Custom Incremental Filter Column (ST)", params.get("custom_incremental_filter_st", ""))
    
    incremental_filter_hs = st.selectbox(
        "Incremental Filter Column (HS)", 
        incremental_filter_options,
        help="__fullLoad: Reads all records from source\nCustom: Specify your own column name",
        index=incremental_filter_options.index(params.get("incremental_filter_hs", "__fullLoad"))
    )
    
    if incremental_filter_hs == "Custom":
        incremental_filter_hs = st.text_input("Custom Incremental Filter Column (HS)", params.get("custom_incremental_filter_hs", ""))
    
    timezone_options = ["UTC", "W. Europe Standard Time"]
    incremental_filter_timezone = st.selectbox(
        "Timezone", 
        timezone_options,
        help="UTC: Coordinated Universal Time\nW. Europe Standard Time: Oslo timezone",
        index=timezone_options.index(params.get("incremental_filter_timezone", "UTC"))
    )
    
    # SCD Configuration
    st.subheader("SCD Configuration")
    
    scd_type_options = ["SCD2", "SCD1", "Transaction only", "Trunc Load", "SCD2 from CT"]
    scd_type = st.selectbox(
        "SCD Type", 
        scd_type_options,
        help="SCD1: Updates existing records\nSCD2: Maintains history with valid from/to dates\nTransaction only: Only inserts records\nTrunc Load: Truncates then loads\nSCD2 from CT: Creates records for all changes using source date column",
        index=scd_type_options.index(params.get("scd_type", "SCD2"))
    )
    
    scd2_columns_options = ["Specify Columns", "__allColumns"]
    scd2_columns_option = st.radio("SCD2 Columns Option", scd2_columns_options, index=scd2_columns_options.index(params.get("scd2_columns_option", "Specify Columns")))
    
    if scd2_columns_option == "Specify Columns":
        scd2_columns = st.text_area("SCD2 Columns (comma-separated)", params.get("scd2_columns", ""), help="Columns to track changes for")
    else:
        scd2_columns = "__allColumns"
    
    # Main Table Creation
    st.subheader("Dimension and Helper Tables")
    create_main_table = st.checkbox("Create main DIM table", help="Creates the main dimension table in DIM schema", value=params.get("create_main_table", False))
    
    if create_main_table:
        main_table_schema = st.text_input("Main Table Schema", params.get("main_table_schema", "DIM"))
        main_table_prefix = st.text_input("Main Table Prefix", params.get("main_table_prefix", "DIM_"))
        
        # Sample columns for the main table
        st.text("Enter placeholder for domain-specific columns:")
        main_table_columns = st.text_area("Table Columns (column name, data type)", 
                                         params.get("main_table_columns", """PARTY_ID bigint NULL,
RECORD_VERSION int NULL,
...... nvarchar(255) NULL"""), 
                                         help="Format: COLUMN_NAME DATA_TYPE NULL/NOT NULL, one per line. Use ...... as placeholder for additional columns.")
    
    # Add create helper table checkbox
    create_helper_table = st.checkbox("Create helper table", 
                                     help="Creates a helper table with business key mapping",
                                     value=params.get("create_helper_table", False))
    
    if create_helper_table:
        helper_schema = st.text_input("Helper Table Schema", params.get("helper_schema", "DF"))
        business_key_column = st.text_input("Business Key Column Name", 
                                           help="The main business key column to include in the helper table",
                                           value=params.get("business_key_column", ""))
    
    # Delete Configuration
    st.subheader("Delete Configuration")
    
    delete_type_options = [None, "SOFT", "HARD"]
    delete_type = st.selectbox(
        "Delete Type", 
        delete_type_options,
        help="SOFT: Source table has a deleted flag\nHARD: Full comparison of business keys",
        index=delete_type_options.index(params.get("delete_type", None))
    )
    
    if delete_type == "SOFT":
        src_delete_column = st.text_input("Source Delete Column", params.get("src_delete_column", "DELETED_FLAG"), help="Source column used to mark records as deleted")
        src_delete_value = st.text_input("Source Delete Value", params.get("src_delete_value", "Y"), help="Value to mark records as deleted")
    else:
        src_delete_column = None
        src_delete_value = None
    
    # Advanced Options
    with st.expander("Advanced Options"):
        # Prescripts and Postscripts
        prescript = st.text_area("Prescript SQL", params.get("prescript", ""), help="SQL to execute before loading data")
        postscript = st.text_area("Postscript SQL", params.get("postscript", ""), help="SQL to execute after loading data")
        
        # Partitioning
        partitions = st.number_input("Partitions", 1, 100, params.get("partitions", 1), help="Number of partitions for SCD2 checks")
        
        # Source Column for Valid Dates
        use_source_column_for_valid_dates = st.checkbox("Use Source Column for Valid Dates", params.get("use_source_column_for_valid_dates", True))
        if use_source_column_for_valid_dates:
            source_column_for_valid_from_date = st.text_input("Source Column for Valid From Date", params.get("source_column_for_valid_from_date", "header__timestamp"), help="Source column used to determine valid from date")
        
        # Source Column for Sorting
        if scd_type == "SCD2 from CT":
            source_column_for_sorting = st.text_input("Source Column for Sorting", params.get("source_column_for_sorting", "header__change_seq"), help="Column for sorting incoming changes")
    
    # Skip Options
    st.subheader("Skip Options")
    skip_st_table = st.checkbox("ST Table Already Exists", params.get("skip_st_table", False))
    skip_hs_table = st.checkbox("HS Table Already Exists", params.get("skip_hs_table", False))
    skip_main_table = st.checkbox("Main Table Already Exists", params.get("skip_main_table", False))
    
    # Generate SQL button in sidebar
    if st.button("Generate SQL Script", type="primary"):
        if not src_table_name or not business_key or (scd2_columns_option == "Specify Columns" and not scd2_columns):
            st.error("Please fill in all required fields: Source Table Name, Business Key, and SCD2 Columns")
        elif not user_initials:
            st.error("Please enter your initials")
        else:
            # Generate unique timestamp for table names
            current_date = datetime.now().strftime("%Y%m%d")
            st.session_state.timestamp = current_date
            st.session_state.table_suffix = f"{user_initials}_{current_date}"
            st.session_state.sql_generated = True
            st.success("SQL Generated Successfully! Check the tabs below.")

# Main content area with tabs
st.header("Generated SQL Deployment Script")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "1. Control Tables Backup", 
    "2. ST Control Table", 
    "3. HS Control Table", 
    "4. Job Control Table",
    "5. Create HS Table",
    "6. ADF Pipeline JSON",
    "7. Dimension and Helper Tables"
])

if st.session_state.sql_generated:
    table_suffix = st.session_state.table_suffix
    
    # Tab 1: Control Tables Backup
    with tab1:
        st.subheader("Step 1: Create Temporary Control Tables")
        
        control_table_stage_sql = f"""-- Make a copy of DWH.CONTROL_TABLE_STAGE
WITH cte AS ( 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('ST_Full_Initial') 	
    UNION ALL 	
    SELECT TOP 1 * FROM DWH.CONTROL_TABLE_STAGE WHERE job_name IN ('ST_Full_Daily') 	
) 	
SELECT *	
INTO sandbox.temp_control_table_st_{table_suffix} FROM cte;
"""
        
        control_table_hs_sql = f"""-- Make a copy of DWH.CONTROL_TABLE_HS
SELECT TOP 1 * 
INTO sandbox.temp_control_table_hs_{table_suffix} 
FROM DWH.CONTROL_TABLE_HS;
"""
        
        job_control_sql = f"""-- Make a copy of DWH.JOB_CONTROL
SELECT * 
INTO sandbox.temp_control_table_job_{table_suffix} 
FROM DWH.JOB_CONTROL 
WHERE job_name IN ('ST_Full_Daily','ST_Full_Initial','HS_Full_Daily','HS_Full_Daily_Control','ST_Placeholder');
"""
        
        tab1_sql = control_table_stage_sql + "\n" + control_table_hs_sql + "\n" + job_control_sql
        st.code(tab1_sql)
    
    # Tab 2: Update ST Control Table
    with tab2:
        st.subheader("Step 2: Update ST Control Table")
        
        # Handle null values properly for SQL generation
        delete_type_sql = "NULL" if delete_type is None else f"'{delete_type}'"
        src_delete_column_sql = "NULL" if src_delete_column is None else f"'{src_delete_column}'"
        src_delete_value_sql = "NULL" if src_delete_value is None else f"'{src_delete_value}'"
        
        st_initial_sql = f"""-- Update temporary control table for stage to reflect Initial Load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = 'ST_Full_Initial',
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
"""
        
        st_daily_sql = f"""-- Update temporary control table for stage to reflect Daily load values
UPDATE sandbox.temp_control_table_st_{table_suffix}
SET 
    job_name = 'ST_Full_Daily',
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
        
        tab2_sql = st_initial_sql + "\n" + st_daily_sql
        st.code(tab2_sql)
    
    # Tab 3: Update HS Control Table
    with tab3:
        st.subheader("Step 3: Update HS Control Table")
        
        # Format prescript and postscript correctly
        prescript_sql = "''" if not prescript else f"'{prescript}'"
        postscript_sql = "''" if not postscript else f"'{postscript}'"
        
        # Format source column options
        use_source_column_value = 1 if use_source_column_for_valid_dates else 0
        source_column_sql = f"'{source_column_for_valid_from_date}'" if use_source_column_for_valid_dates else "NULL"
        
        # Add sorting column if SCD2 from CT
        sorting_column_clause = ""
        if scd_type == "SCD2 from CT" and 'source_column_for_sorting' in locals():
            sorting_column_clause = f"""
    source_column_for_sorting = '{source_column_for_sorting}',"""
        
        hs_sql = f"""-- Update temporary control table for historic stage to reflect daily load values
UPDATE sandbox.temp_control_table_hs_{table_suffix}
SET job_name = 'HS_Full_Daily',
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
        
        tab3_sql = hs_sql
        st.code(tab3_sql)
    
    # Tab 4: Update Job Control
    with tab4:
        st.subheader("Step 4: Update Job Control Table")
        
        job_control_update_sql = f"""-- Update the control table so that the jobs are set to STATUS='SUCCESS'
UPDATE sandbox.temp_control_table_job_{table_suffix}
SET 
    STATUS = 'SUCCESS',
    LAST_LOAD_DATE = '1970-01-01',
    JOB_INTERVAL_IN_MINUTES = 0
WHERE job_name IN ('HS_Full_Daily','ST_Full_Daily','HS_Full_Daily_Control','ST_Full_Initial');
"""
        
        tab4_sql = job_control_update_sql
        st.code(tab4_sql)
        
        st.markdown("""
        In the next step, we will create the ADF pipeline JSON that will use these control table configurations to orchestrate the data loading process.
        """)
    
    # Tab 5: Create HS Tables
    with tab5:
        st.subheader("Step 5: Create HS Table")
        
        hs_create_sql = ""
        if not skip_hs_table:
            hs_create_sql = f"""-- Create the HS table with technical columns
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
        else:
            hs_create_sql = f"-- HS Table creation skipped as per user selection"
        
        tab5_sql = hs_create_sql
        st.code(tab5_sql)
        
        st.markdown("""
        **Note:** If you want a quicker way to get to the HS tables, you can run the initial load with an invalid HS job name. 
        This will only run the stage part of the job and then fail. Then create the HS table with this script.
        """)
    
    # Tab 6: ADF Pipeline JSON
    with tab6:
        st.subheader("Step 6: ADF Pipeline JSON")
        
        # Generate the ADF pipeline JSON for both initial and daily loads
        adf_pipeline_name_initial = f"pl_StageAndHistoricStageInitialLoad_{src_table_name}"
        adf_pipeline_name_daily = f"pl_StageAndHistoricStageDailyLoad_{src_table_name}"
        
        # Create initial load JSON
        adf_json_initial = {
            "name": adf_pipeline_name_initial,
            "properties": {
                "activities": [
                    {
                        "name": "Initial Stage and HS",
                        "type": "ExecutePipeline",
                        "dependsOn": [],
                        "policy": {
                            "secureInput": False
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "pipeline": {
                                "referenceName": "pl_framework_StageAndHSLoop",
                                "type": "PipelineReference"
                            },
                            "waitOnCompletion": True,
                            "parameters": {
                                "pStopDate": {
                                    "value": "@formatDateTime(addDays(utcNow(),1),'yyyy-MM-dd HH:mm:ss')",
                                    "type": "Expression"
                                },
                                "pSTJob": "ST_Full_Initial",
                                "pHSJob": "HS_Full_Initial",
                                "pJobControlSchema": "sandbox",
                                "pJobControlTable": f"temp_control_table_job_{table_suffix}",
                                "pSTTablesControlSchema": "sandbox",
                                "pSTTablesControlTable": f"temp_control_table_st_{table_suffix}",
                                "pHSTablesControlSchema": "sandbox",
                                "pHSTablesControlTable": f"temp_control_table_hs_{table_suffix}",
                                "pLoopJob": "HS_Full_Initial_Control",
                                "pLogSchema": "DWH",
                                "pLogTableJobLevel": "JOB_LOG",
                                "pLogTableTableLevel": "JOB_TABLES_LOG",
                                "pIntialLoad": True
                            }
                        }
                    }
                ],
                "folder": {
                    "name": "Deployment and initial load"
                },
                "annotations": []
            }
        }
        
        # Create daily load JSON
        adf_json_daily = {
            "name": adf_pipeline_name_daily,
            "properties": {
                "activities": [
                    {
                        "name": "Daily Stage and HS",
                        "type": "ExecutePipeline",
                        "dependsOn": [],
                        "policy": {
                            "secureInput": False
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "pipeline": {
                                "referenceName": "pl_framework_StageAndHSLoop",
                                "type": "PipelineReference"
                            },
                            "waitOnCompletion": True,
                            "parameters": {
                                "pStopDate": {
                                    "value": "@formatDateTime(addDays(utcNow(),1),'yyyy-MM-dd HH:mm:ss')",
                                    "type": "Expression"
                                },
                                "pSTJob": "ST_Full_Daily",
                                "pHSJob": "HS_Full_Daily",
                                "pJobControlSchema": "sandbox",
                                "pJobControlTable": f"temp_control_table_job_{table_suffix}",
                                "pSTTablesControlSchema": "sandbox",
                                "pSTTablesControlTable": f"temp_control_table_st_{table_suffix}",
                                "pHSTablesControlSchema": "sandbox",
                                "pHSTablesControlTable": f"temp_control_table_hs_{table_suffix}",
                                "pLoopJob": "HS_Full_Daily_Control",
                                "pLogSchema": "DWH",
                                "pLogTableJobLevel": "JOB_LOG",
                                "pLogTableTableLevel": "JOB_TABLES_LOG",
                                "pIntialLoad": False
                            }
                        }
                    },
                    {
                        "name": "Stage Deletes",
                        "type": "ExecutePipeline",
                        "state": "Inactive",
                        "onInactiveMarkAs": "Succeeded",
                        "dependsOn": [
                            {
                                "activity": "Daily Stage and HS",
                                "dependencyConditions": [
                                    "Succeeded"
                                ]
                            }
                        ],
                        "policy": {
                            "secureInput": False
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "pipeline": {
                                "referenceName": "pl_framework_StageDeleteLoop",
                                "type": "PipelineReference"
                            },
                            "waitOnCompletion": True,
                            "parameters": {
                                "pJobName": "ST_Full_Daily",
                                "pDataControlTable": "CONTROL_TABLE_STAGE",
                                "pDataControlSchema": "DWH"
                            }
                        }
                    },
                    {
                        "name": "Update Delete flags",
                        "type": "ExecutePipeline",
                        "state": "Inactive",
                        "onInactiveMarkAs": "Succeeded",
                        "dependsOn": [
                            {
                                "activity": "Stage Deletes",
                                "dependencyConditions": [
                                    "Succeeded"
                                ]
                            }
                        ],
                        "policy": {
                            "secureInput": False
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "pipeline": {
                                "referenceName": "pl_framework_UpdateDeleteFlag",
                                "type": "PipelineReference"
                            },
                            "waitOnCompletion": True,
                            "parameters": {
                                "pJobName": "HS_Full_Daily",
                                "pDataControlTable": "CONTROL_TABLE_HS",
                                "pDataControlSchema": "DWH",
                                "pStageControlTable": "CONTROL_TABLE_STAGE",
                                "pStageControlSchema": "DWH",
                                "pStageJobName": "ST_Full_Daily"
                            }
                        }
                    },
                    {
                        "name": "BK_preload",
                        "type": "ExecutePipeline",
                        "state": "Inactive",
                        "onInactiveMarkAs": "Succeeded",
                        "dependsOn": [
                            {
                                "activity": "Update Delete flags",
                                "dependencyConditions": [
                                    "Succeeded"
                                ]
                            }
                        ],
                        "policy": {
                            "secureInput": False
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "pipeline": {
                                "referenceName": "pl_BKPreload_Test",
                                "type": "PipelineReference"
                            },
                            "waitOnCompletion": True
                        }
                    }
                ],
                "folder": {
                    "name": "Scheduling"
                },
                "annotations": []
            }
        }
        
        # Convert the Python dictionaries to formatted JSON strings
        adf_json_str_initial = json.dumps(adf_json_initial, indent=4)
        adf_json_str_daily = json.dumps(adf_json_daily, indent=4)
        
        # Create tabs for initial and daily load JSONs
        initial_tab, daily_tab = st.tabs(["Initial Load Pipeline", "Daily Load Pipeline"])
        
        with initial_tab:
            st.markdown("### Initial Load Pipeline")
            st.code(adf_json_str_initial, language="json")
            st.download_button(
                label="Download Initial Load Pipeline JSON",
                data=adf_json_str_initial,
                file_name=f"{adf_pipeline_name_initial}.json",
                mime="application/json",
                key="download_adf_json_initial",
            )
        
        with daily_tab:
            st.markdown("### Daily Load Pipeline")
            st.code(adf_json_str_daily, language="json")
            st.download_button(
                label="Download Daily Load Pipeline JSON",
                data=adf_json_str_daily,
                file_name=f"{adf_pipeline_name_daily}.json",
                mime="application/json",
                key="download_adf_json_daily",
            )
        
        # Add instructions for pasting into ADF
        st.markdown("""
        ### Instructions for Pasting into ADF
        
        1. Open Azure Data Factory dev
        2. For Initial Load:
           - Navigate to the "Deployment and initial load" folder
           - Create a new pipeline
           - Rename it to match the initial load pipeline name
           - Click the "Code" button in the top right corner
           - Delete all existing code in the editor
           - Paste the Initial Load JSON code
           - Click "Apply"
           - Save the pipeline
        
        3. For Daily Load:
           - Navigate to the "Scheduling" folder
           - Create a new pipeline
           - Rename it to match the daily load pipeline name
           - Click the "Code" button in the top right corner
           - Delete all existing code in the editor
           - Paste the Daily Load JSON code
           - Click "Apply"
           - Save the pipeline
        
        4. After successful initial load:
           - Update the control tables to use the daily load job names
           - The daily load pipeline will then use these updated names
        
        The pipelines will use the temporary control tables created in the previous steps.
        """)
        
        st.markdown("""
        In the next step, we will create the dimension and helper tables that will store the final data.
        """)
    
    # Tab 7: Dimension and Helper Tables
    with tab7:
        st.subheader("Step 7: Create Dimension and Helper Tables")
        
        # Helper Table Creation
        if create_helper_table and business_key_column:
            # Extract the table name without prefix for helper table naming
            base_table_name = src_table_name
            if "_" in src_table_name:
                base_table_name = src_table_name.split("_", 1)[1] if src_table_name.count("_") > 0 else src_table_name
            
            helper_table_name = f"HLP_BK_{base_table_name}"
            identity_column_name = f"BK_{base_table_name}"
            
            helper_table_sql = f"""SET ANSI_NULLS ON
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
            st.code(helper_table_sql)
        else:
            st.info("Helper table creation was not selected. Check the 'Create helper table' option in the Dimension and Helper Tables section to generate the helper table SQL.")
    
        # Main Table Creation
        if create_main_table and not skip_main_table:
            # Extract the base table name without prefix for main table naming
            base_table_name = src_table_name
            if "_" in src_table_name:
                base_table_name = src_table_name.split("_", 1)[1] if src_table_name.count("_") > 0 else src_table_name
            
            # Prepare table name, with user-specified prefix if provided
            if main_table_prefix:
                main_table_name = f"{main_table_prefix}{base_table_name}"
            else:
                main_table_name = f"DIM_{base_table_name}"
            
            # Primary key and business key column names
            pk_column_name = f"PK_{main_table_name}"
            bk_column_name = f"BK_{base_table_name}"
            
            # Process the user's column definitions
            column_defs = []
            for line in main_table_columns.strip().split('\n'):
                if line.strip():
                    column_defs.append(f"\t{line.strip()}")
            
            # Generate main table SQL
            main_table_sql = f"""SET ANSI_NULLS ON
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
            st.code(main_table_sql)
        else:
            if skip_main_table:
                st.info("Main table creation skipped as per user selection.")
            else:
                st.info("Main table creation was not selected. Check the 'Create main DIM table' option to generate the main table SQL.")
    
    # Prepare complete SQL script for download
    # Add the helper table and main table SQL to the complete script if they were selected
    helper_table_section = ""
    if create_helper_table and business_key_column:
        helper_table_section = f"""
---------------------------------------------------------
-- STEP 7: CREATE HELPER TABLE
---------------------------------------------------------
{helper_table_sql}
"""

    main_table_section = ""
    if create_main_table and not skip_main_table:
        main_table_section = f"""
---------------------------------------------------------
-- STEP 8: CREATE MAIN TABLE
---------------------------------------------------------
{main_table_sql}
"""

    complete_sql = f"""-- Generated SQL Deployment Script for {src_table_name}
-- Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- This script contains all steps needed for deploying {src_table_name} to the data warehouse.
-- Created by: {user_initials.upper()}

---------------------------------------------------------
-- STEP 1: CREATE TEMPORARY CONTROL TABLES
---------------------------------------------------------
{tab1_sql}

---------------------------------------------------------
-- STEP 2: UPDATE ST CONTROL TABLE
---------------------------------------------------------
{tab2_sql}

---------------------------------------------------------
-- STEP 3: UPDATE HS CONTROL TABLE
---------------------------------------------------------
{tab3_sql}

---------------------------------------------------------
-- STEP 4: UPDATE JOB CONTROL TABLE
---------------------------------------------------------
{tab4_sql}

---------------------------------------------------------
-- STEP 5: CREATE HS TABLE
---------------------------------------------------------
{tab5_sql}

---------------------------------------------------------
-- STEP 6: ADF PIPELINE JSON
---------------------------------------------------------
{helper_table_section}{main_table_section}
-- End of script
"""
    
    # Store the complete SQL in session state
    st.session_state.all_sql = complete_sql
    
    # Create a download button for the complete SQL script
    st.markdown("---")
    st.subheader("Download Complete SQL Script")
    
    # Create a buffer for the SQL content
    sql_file = io.StringIO()
    sql_file.write(complete_sql)
    
    # Download button
    st.download_button(
        label="Download SQL Script",
        data=sql_file.getvalue(),
        file_name=f"deploy_{src_table_name}_{table_suffix}.sql",
        mime="text/plain",
        key="download_sql",
    )
else:
    st.info("Fill in the required fields in the sidebar and click 'Generate SQL Script' to see the deployment steps.")

# Footer
st.markdown("---")
st.caption("DWH Table Deployment Helper - Streamlit App") 