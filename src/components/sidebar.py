import streamlit as st
from datetime import datetime
from src.utils.parameters import export_parameters, import_parameters, get_current_params
from src.config.constants import (
    SOURCE_SYSTEM_OPTIONS, DEFAULT_VALUES, SCD_TYPE_OPTIONS,
    SCD2_COLUMNS_OPTIONS, DELETE_TYPE_OPTIONS, TIMEZONE_OPTIONS,
    INCREMENTAL_FILTER_OPTIONS
)

def render_import_export_section():
    """Render the import/export section in the sidebar"""
    with st.expander("Import/Export Parameters"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Export Parameters")
            if st.button("Export Current Parameters"):
                # Get all current parameter values
                current_params = get_current_params(
                    st.session_state.user_initials,
                    st.session_state.source_system_initial,
                    st.session_state.source_system_daily,
                    st.session_state.src_schema_name,
                    st.session_state.src_table_name,
                    st.session_state.tgt_schema_name_st,
                    st.session_state.tgt_table_name_st,
                    st.session_state.tgt_schema_name_hs,
                    st.session_state.tgt_table_name_hs,
                    st.session_state.business_key,
                    st.session_state.primary_key,
                    st.session_state.incremental_filter_st,
                    st.session_state.incremental_filter_hs,
                    st.session_state.incremental_filter_timezone,
                    st.session_state.scd_type,
                    st.session_state.scd2_columns_option,
                    st.session_state.scd2_columns,
                    st.session_state.create_helper_table,
                    st.session_state.helper_schema,
                    st.session_state.business_key_column,
                    st.session_state.delete_type,
                    st.session_state.src_delete_column,
                    st.session_state.src_delete_value,
                    st.session_state.prescript,
                    st.session_state.postscript,
                    st.session_state.partitions,
                    st.session_state.use_source_column_for_valid_dates,
                    st.session_state.source_column_for_valid_from_date,
                    st.session_state.source_column_for_sorting,
                    st.session_state.create_main_table,
                    st.session_state.main_table_schema,
                    st.session_state.main_table_prefix,
                    st.session_state.main_table_columns,
                    st.session_state.skip_st_table,
                    st.session_state.skip_hs_table,
                    st.session_state.skip_main_table
                )
                params_json = export_parameters(current_params)
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
                    imported_params = import_parameters(params_str)
                    st.success("Parameters loaded successfully! Click 'Apply Parameters' to use them.")
                    if st.button("Apply Parameters"):
                        st.session_state.imported_params = imported_params
                        st.experimental_rerun()
                except json.JSONDecodeError:
                    st.error("Invalid JSON format")

def render_source_table_section(params):
    """Render the source table configuration section"""
    st.subheader("Source Table Configuration")
    
    source_system_initial = st.selectbox(
        "Source System (Initial)", 
        SOURCE_SYSTEM_OPTIONS,
        index=SOURCE_SYSTEM_OPTIONS.index(params.get("source_system_initial", DEFAULT_VALUES["source_system_initial"])),
        help="INS_temporal: INS Tables with system version control\nTIA: Reinsurance application\nReplicate_Full: Initial load tables\nReplicate_CDC: CDC tables (latest change per business key)\nProfisee: Profisee production database\nReplicate_CDC_AllTransactions: All transactions from CT table\nReplicate_CDC_AllTransactions_fromArchive: Read from view combining archives"
    )
    
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
        SOURCE_SYSTEM_OPTIONS,
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
    
    src_schema_name = st.text_input("Source Schema Name", params.get("src_schema_name", DEFAULT_VALUES["src_schema_name"]))
    src_table_name = st.text_input("Source Table Name", params.get("src_table_name", ""))
    src_table_name_ct = f"{src_table_name}__ct" if src_table_name and source_system_daily == "Replicate_CDC" else src_table_name
    
    return source_system_initial, source_system_daily, src_schema_name, src_table_name, src_table_name_ct

def render_target_table_section(params, src_table_name):
    """Render the target table configuration section"""
    st.subheader("Target Table Configuration")
    tgt_schema_name_st = st.text_input("ST Schema Name", params.get("tgt_schema_name_st", DEFAULT_VALUES["tgt_schema_name_st"]))
    tgt_table_name_st = st.text_input("ST Table Name", f"ST_{src_table_name}" if src_table_name else params.get("tgt_table_name_st", ""))
    
    tgt_schema_name_hs = st.text_input("HS Schema Name", params.get("tgt_schema_name_hs", DEFAULT_VALUES["tgt_schema_name_hs"]))
    tgt_table_name_hs = st.text_input("HS Table Name", f"HS_{src_table_name}" if src_table_name else params.get("tgt_table_name_hs", ""))
    
    return tgt_schema_name_st, tgt_table_name_st, tgt_schema_name_hs, tgt_table_name_hs

def render_key_columns_section(params):
    """Render the key columns configuration section"""
    st.subheader("Key Columns Configuration")
    business_key = st.text_input("Business Key (comma-separated)", params.get("business_key", ""), help="List of columns that identify a unique row in the source")
    primary_key = st.text_input("Primary Key", params.get("primary_key", DEFAULT_VALUES["primary_key"]), help="Name of the Primary key field in the target table. Usually an identity column")
    
    return business_key, primary_key

def render_incremental_load_section(params):
    """Render the incremental load configuration section"""
    st.subheader("Incremental Load Configuration")
    
    incremental_filter_st = st.selectbox(
        "Incremental Filter Column (ST)", 
        INCREMENTAL_FILTER_OPTIONS,
        help="__fullLoad: Reads all records from source\nCustom: Specify your own column name",
        index=INCREMENTAL_FILTER_OPTIONS.index(params.get("incremental_filter_st", DEFAULT_VALUES["incremental_filter_st"]))
    )
    
    if incremental_filter_st == "Custom":
        incremental_filter_st = st.text_input("Custom Incremental Filter Column (ST)", params.get("custom_incremental_filter_st", ""))
    
    incremental_filter_hs = st.selectbox(
        "Incremental Filter Column (HS)", 
        INCREMENTAL_FILTER_OPTIONS,
        help="__fullLoad: Reads all records from source\nCustom: Specify your own column name",
        index=INCREMENTAL_FILTER_OPTIONS.index(params.get("incremental_filter_hs", DEFAULT_VALUES["incremental_filter_hs"]))
    )
    
    if incremental_filter_hs == "Custom":
        incremental_filter_hs = st.text_input("Custom Incremental Filter Column (HS)", params.get("custom_incremental_filter_hs", ""))
    
    incremental_filter_timezone = st.selectbox(
        "Timezone", 
        TIMEZONE_OPTIONS,
        help="UTC: Coordinated Universal Time\nW. Europe Standard Time: Oslo timezone",
        index=TIMEZONE_OPTIONS.index(params.get("incremental_filter_timezone", DEFAULT_VALUES["incremental_filter_timezone"]))
    )
    
    return incremental_filter_st, incremental_filter_hs, incremental_filter_timezone

def render_scd_section(params):
    """Render the SCD configuration section"""
    st.subheader("SCD Configuration")
    
    scd_type = st.selectbox(
        "SCD Type", 
        SCD_TYPE_OPTIONS,
        help="SCD1: Updates existing records\nSCD2: Maintains history with valid from/to dates\nTransaction only: Only inserts records\nTrunc Load: Truncates then loads\nSCD2 from CT: Creates records for all changes using source date column",
        index=SCD_TYPE_OPTIONS.index(params.get("scd_type", DEFAULT_VALUES["scd_type"]))
    )
    
    scd2_columns_option = st.radio("SCD2 Columns Option", SCD2_COLUMNS_OPTIONS, index=SCD2_COLUMNS_OPTIONS.index(params.get("scd2_columns_option", DEFAULT_VALUES["scd2_columns_option"])))
    
    if scd2_columns_option == "Specify Columns":
        scd2_columns = st.text_area("SCD2 Columns (comma-separated)", params.get("scd2_columns", ""), help="Columns to track changes for")
    else:
        scd2_columns = "__allColumns"
    
    return scd_type, scd2_columns_option, scd2_columns

def render_dimension_helper_section(params):
    """Render the dimension and helper tables section"""
    st.subheader("Dimension and Helper Tables")
    create_main_table = st.checkbox("Create main DIM table", help="Creates the main dimension table in DIM schema", value=params.get("create_main_table", False))
    
    main_table_schema = ""
    main_table_prefix = ""
    main_table_columns = ""
    if create_main_table:
        main_table_schema = st.text_input("Main Table Schema", params.get("main_table_schema", DEFAULT_VALUES["main_table_schema"]))
        main_table_prefix = st.text_input("Main Table Prefix", params.get("main_table_prefix", DEFAULT_VALUES["main_table_prefix"]))
        
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
    
    helper_schema = ""
    business_key_column = ""
    if create_helper_table:
        helper_schema = st.text_input("Helper Table Schema", params.get("helper_schema", DEFAULT_VALUES["helper_schema"]))
        business_key_column = st.text_input("Business Key Column Name", 
                                           help="The main business key column to include in the helper table",
                                           value=params.get("business_key_column", ""))
    
    return (create_main_table, main_table_schema, main_table_prefix, main_table_columns,
            create_helper_table, helper_schema, business_key_column)

def render_delete_section(params):
    """Render the delete configuration section"""
    st.subheader("Delete Configuration")
    
    delete_type = st.selectbox(
        "Delete Type", 
        DELETE_TYPE_OPTIONS,
        help="SOFT: Source table has a deleted flag\nHARD: Full comparison of business keys",
        index=DELETE_TYPE_OPTIONS.index(params.get("delete_type", None))
    )
    
    src_delete_column = None
    src_delete_value = None
    if delete_type == "SOFT":
        src_delete_column = st.text_input("Source Delete Column", params.get("src_delete_column", DEFAULT_VALUES["src_delete_column"]), help="Source column used to mark records as deleted")
        src_delete_value = st.text_input("Source Delete Value", params.get("src_delete_value", DEFAULT_VALUES["src_delete_value"]), help="Value to mark records as deleted")
    
    return delete_type, src_delete_column, src_delete_value

def render_advanced_options(params):
    """Render the advanced options section"""
    with st.expander("Advanced Options"):
        # Prescripts and Postscripts
        prescript = st.text_area("Prescript SQL", params.get("prescript", ""), help="SQL to execute before loading data")
        postscript = st.text_area("Postscript SQL", params.get("postscript", ""), help="SQL to execute after loading data")
        
        # Partitioning
        partitions = st.number_input("Partitions", 1, 100, params.get("partitions", DEFAULT_VALUES["partitions"]), help="Number of partitions for SCD2 checks")
        
        # Source Column for Valid Dates
        use_source_column_for_valid_dates = st.checkbox("Use Source Column for Valid Dates", params.get("use_source_column_for_valid_dates", DEFAULT_VALUES["use_source_column_for_valid_dates"]))
        source_column_for_valid_from_date = ""
        if use_source_column_for_valid_dates:
            source_column_for_valid_from_date = st.text_input("Source Column for Valid From Date", params.get("source_column_for_valid_from_date", DEFAULT_VALUES["source_column_for_valid_from_date"]), help="Source column used to determine valid from date")
        
        # Source Column for Sorting
        source_column_for_sorting = ""
        if params.get("scd_type") == "SCD2 from CT":
            source_column_for_sorting = st.text_input("Source Column for Sorting", params.get("source_column_for_sorting", DEFAULT_VALUES["source_column_for_sorting"]), help="Column for sorting incoming changes")
        
        return (prescript, postscript, partitions, use_source_column_for_valid_dates,
                source_column_for_valid_from_date, source_column_for_sorting)

def render_skip_options(params):
    """Render the skip options section"""
    st.subheader("Skip Options")
    skip_st_table = st.checkbox("ST Table Already Exists", params.get("skip_st_table", False))
    skip_hs_table = st.checkbox("HS Table Already Exists", params.get("skip_hs_table", False))
    skip_main_table = st.checkbox("Main Table Already Exists", params.get("skip_main_table", False))
    
    return skip_st_table, skip_hs_table, skip_main_table

def render_sidebar():
    """Render the complete sidebar"""
    st.header("Table Information")
    
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
    
    # Render all sections
    render_import_export_section()
    
    source_system_initial, source_system_daily, src_schema_name, src_table_name, src_table_name_ct = render_source_table_section(params)
    tgt_schema_name_st, tgt_table_name_st, tgt_schema_name_hs, tgt_table_name_hs = render_target_table_section(params, src_table_name)
    business_key, primary_key = render_key_columns_section(params)
    incremental_filter_st, incremental_filter_hs, incremental_filter_timezone = render_incremental_load_section(params)
    scd_type, scd2_columns_option, scd2_columns = render_scd_section(params)
    create_main_table, main_table_schema, main_table_prefix, main_table_columns, create_helper_table, helper_schema, business_key_column = render_dimension_helper_section(params)
    delete_type, src_delete_column, src_delete_value = render_delete_section(params)
    prescript, postscript, partitions, use_source_column_for_valid_dates, source_column_for_valid_from_date, source_column_for_sorting = render_advanced_options(params)
    skip_st_table, skip_hs_table, skip_main_table = render_skip_options(params)
    
    # Store all values in session state
    st.session_state.update({
        "source_system_initial": source_system_initial,
        "source_system_daily": source_system_daily,
        "src_schema_name": src_schema_name,
        "src_table_name": src_table_name,
        "src_table_name_ct": src_table_name_ct,
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
        "scd2_columns": scd2_columns,
        "create_main_table": create_main_table,
        "main_table_schema": main_table_schema,
        "main_table_prefix": main_table_prefix,
        "main_table_columns": main_table_columns,
        "create_helper_table": create_helper_table,
        "helper_schema": helper_schema,
        "business_key_column": business_key_column,
        "delete_type": delete_type,
        "src_delete_column": src_delete_column,
        "src_delete_value": src_delete_value,
        "prescript": prescript,
        "postscript": postscript,
        "partitions": partitions,
        "use_source_column_for_valid_dates": use_source_column_for_valid_dates,
        "source_column_for_valid_from_date": source_column_for_valid_from_date,
        "source_column_for_sorting": source_column_for_sorting,
        "skip_st_table": skip_st_table,
        "skip_hs_table": skip_hs_table,
        "skip_main_table": skip_main_table
    })
    
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