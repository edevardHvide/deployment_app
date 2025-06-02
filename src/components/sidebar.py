import streamlit as st
from datetime import datetime
import json
from src.utils.parameters import export_parameters, import_parameters, get_current_params
from src.config.constants import (
    SOURCE_SYSTEM_OPTIONS, DEFAULT_VALUES, SCD_TYPE_OPTIONS,
    SCD2_COLUMNS_OPTIONS, DELETE_TYPE_OPTIONS, TIMEZONE_OPTIONS,
    INCREMENTAL_FILTER_OPTIONS
)

def render_import_export_section():
    """Render the import parameters section in the sidebar"""
    st.subheader("Import/Export Parameters")
    
    # New Export for Deployers button
    if st.button("Export for Deployers", help="Create a configuration file for deployers"):
        params = get_current_params()
        if 'src_table_name' in params and params['src_table_name']:
            # Show table suffix info if available
            if 'table_suffix' in params and params['table_suffix']:
                st.info(f"💾 Table suffix `{params['table_suffix']}` will be preserved in the export.")
            
            params_json = export_parameters(params)
            st.download_button(
                label=f"Download Configuration",
                data=params_json,
                file_name=f"deploy_{params['src_table_name']}_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json",
                key="download_params_deployer",
                help="Download configuration file for deployers"
            )
            st.info("Download this configuration file and provide it to the deployment team.")
        else:
            st.warning("Please fill in the source table name first.")
    
    st.markdown("---")
    
    uploaded_file = st.file_uploader("Upload Parameters File", type=['json'])
    if uploaded_file is not None:
        params_str = uploaded_file.getvalue().decode()
        try:
            imported_params = import_parameters(params_str)
            if imported_params:
                st.success("Parameters loaded successfully! Click 'Apply Parameters' to use them.")
                if st.button("Apply Parameters"):
                    # Store all imported parameters directly in session state
                    for key, value in imported_params.items():
                        if key not in ["export_timestamp", "app_version"]:  # Skip metadata
                            st.session_state[key] = value
                    st.success("Parameters applied successfully!")
        except Exception as e:
            st.error(f"Error importing parameters: {str(e)}")
            
    # Allow developers to switch roles
    st.markdown("---")
    if st.button("Switch to Deployer View"):
        st.session_state.user_role = "deployer"
        st.rerun()

def render_source_table_section():
    """Render the source table configuration section"""
    st.subheader("Source Table Configuration")
    
    source_system_initial = st.selectbox(
        "Source System (Initial)", 
        SOURCE_SYSTEM_OPTIONS,
        index=SOURCE_SYSTEM_OPTIONS.index(st.session_state.get("source_system_initial", DEFAULT_VALUES["source_system_initial"])),
        help="INS_temporal: INS Tables with system version control\nTIA: Reinsurance application\nReplicate_Full: Initial load tables\nReplicate_CDC: CDC tables (latest change per business key)\nProfisee: Profisee production database\nReplicate_CDC_AllTransactions: All transactions from CT table\nReplicate_CDC_AllTransactions_fromArchive: Read from view combining archives"
    )
    
    source_system_daily = st.selectbox(
        "Source System (Daily)", 
        SOURCE_SYSTEM_OPTIONS,
        index=SOURCE_SYSTEM_OPTIONS.index(st.session_state.get("source_system_daily", DEFAULT_VALUES["source_system_daily"]))
    )
    
    src_schema_name = st.text_input("Source Schema Name", st.session_state.get("src_schema_name", DEFAULT_VALUES["src_schema_name"]))
    src_table_name = st.text_input("Source Table Name", st.session_state.get("src_table_name", ""))
    
    # Calculate the CT table name if needed
    if src_table_name and source_system_daily == "Replicate_CDC":
        src_table_name_ct = f"{src_table_name}__ct"
    else:
        src_table_name_ct = src_table_name
    
    return source_system_initial, source_system_daily, src_schema_name, src_table_name, src_table_name_ct

def render_target_table_section(src_table_name):
    """Render the target table configuration section"""
    st.subheader("Target Table Configuration")
    
    tgt_schema_name_st = st.text_input(
        "ST Schema Name", 
        st.session_state.get("tgt_schema_name_st", DEFAULT_VALUES["tgt_schema_name_st"])
    )
    
    # Set default ST table name based on source table or existing value
    default_st_name = f"ST_{src_table_name}" if src_table_name else ""
    tgt_table_name_st = st.text_input(
        "ST Table Name", 
        st.session_state.get("tgt_table_name_st", default_st_name)
    )
    
    tgt_schema_name_hs = st.text_input(
        "HS Schema Name", 
        st.session_state.get("tgt_schema_name_hs", DEFAULT_VALUES["tgt_schema_name_hs"])
    )
    
    # Set default HS table name based on source table or existing value
    default_hs_name = f"HS_{src_table_name}" if src_table_name else ""
    tgt_table_name_hs = st.text_input(
        "HS Table Name", 
        st.session_state.get("tgt_table_name_hs", default_hs_name)
    )
    
    return tgt_schema_name_st, tgt_table_name_st, tgt_schema_name_hs, tgt_table_name_hs

def render_key_columns_section():
    """Render the key columns configuration section"""
    st.subheader("Key Columns Configuration")
    business_key = st.text_input(
        "Business Key (comma-separated)", 
        st.session_state.get("business_key", ""),
        help="List of columns that identify a unique row in the source"
    )
    primary_key = st.text_input(
        "HS Table Primary Key", 
        st.session_state.get("primary_key", DEFAULT_VALUES["primary_key"]), 
        help="Name of the Primary key field in the HS table. Usually TC_ROW_ID identity column"
    )
    
    return business_key, primary_key

def render_incremental_load_section():
    """Render the incremental load configuration section"""
    st.subheader("Incremental Load Configuration")
    
    # Get default values from session state or constants
    default_inc_filter_st = st.session_state.get("incremental_filter_st", DEFAULT_VALUES["incremental_filter_st"])
    default_inc_filter_hs = st.session_state.get("incremental_filter_hs", DEFAULT_VALUES["incremental_filter_hs"])
    default_inc_timezone = st.session_state.get("incremental_filter_timezone", DEFAULT_VALUES["incremental_filter_timezone"])
    
    # Handle custom values that might not be in the options list
    if default_inc_filter_st in INCREMENTAL_FILTER_OPTIONS:
        inc_filter_st_index = INCREMENTAL_FILTER_OPTIONS.index(default_inc_filter_st)
        inc_filter_st_custom = ""
    else:
        inc_filter_st_index = INCREMENTAL_FILTER_OPTIONS.index("Custom")
        inc_filter_st_custom = default_inc_filter_st
    
    if default_inc_filter_hs in INCREMENTAL_FILTER_OPTIONS:
        inc_filter_hs_index = INCREMENTAL_FILTER_OPTIONS.index(default_inc_filter_hs)
        inc_filter_hs_custom = ""
    else:
        inc_filter_hs_index = INCREMENTAL_FILTER_OPTIONS.index("Custom")
        inc_filter_hs_custom = default_inc_filter_hs
    
    incremental_filter_st = st.selectbox(
        "Incremental Filter Column (ST)", 
        INCREMENTAL_FILTER_OPTIONS,
        index=inc_filter_st_index,
        help="__fullLoad: Reads all records from source\nCustom: Specify your own column name"
    )
    
    if incremental_filter_st == "Custom":
        incremental_filter_st = st.text_input("Custom Incremental Filter Column (ST)", inc_filter_st_custom)
    
    incremental_filter_hs = st.selectbox(
        "Incremental Filter Column (HS)", 
        INCREMENTAL_FILTER_OPTIONS,
        index=inc_filter_hs_index,
        help="__fullLoad: Reads all records from source\nCustom: Specify your own column name"
    )
    
    if incremental_filter_hs == "Custom":
        incremental_filter_hs = st.text_input("Custom Incremental Filter Column (HS)", inc_filter_hs_custom)
    
    incremental_filter_timezone = st.selectbox(
        "Timezone", 
        TIMEZONE_OPTIONS,
        index=TIMEZONE_OPTIONS.index(default_inc_timezone),
        help="UTC: Coordinated Universal Time\nW. Europe Standard Time: Oslo timezone"
    )
    
    return incremental_filter_st, incremental_filter_hs, incremental_filter_timezone

def render_scd_section():
    """Render the SCD configuration section"""
    st.subheader("SCD Configuration")
    
    default_scd_type = st.session_state.get("scd_type", DEFAULT_VALUES["scd_type"])
    default_scd2_option = st.session_state.get("scd2_columns_option", DEFAULT_VALUES["scd2_columns_option"])
    default_scd2_columns = st.session_state.get("scd2_columns", "")
    
    scd_type = st.selectbox(
        "SCD Type", 
        SCD_TYPE_OPTIONS,
        index=SCD_TYPE_OPTIONS.index(default_scd_type),
        help="SCD1: Updates existing records\nSCD2: Maintains history with valid from/to dates\nTransaction only: Only inserts records\nTrunc Load: Truncates then loads\nSCD2 from CT: Creates records for all changes using source date column"
    )
    
    scd2_columns_option = st.radio(
        "SCD2 Columns Option", 
        SCD2_COLUMNS_OPTIONS, 
        index=SCD2_COLUMNS_OPTIONS.index(default_scd2_option)
    )
    
    if scd2_columns_option == "Specify Columns":
        scd2_columns = st.text_area("SCD2 Columns (comma-separated)", default_scd2_columns, help="Columns to track changes for")
    else:
        scd2_columns = "__allColumns"
    
    return scd_type, scd2_columns_option, scd2_columns

def render_dimension_helper_section():
    """Render the dimension and helper tables section"""
    st.subheader("Dimension and Helper Tables")
    
    create_main_table = st.checkbox(
        "Create main DIM table", 
        help="Creates the main dimension table in DIM schema", 
        value=st.session_state.get("create_main_table", False)
    )
    
    main_table_schema = ""
    main_table_name = ""
    main_table_columns = ""
    if create_main_table:
        main_table_schema = st.text_input(
            "Main Table Schema", 
            st.session_state.get("main_table_schema", DEFAULT_VALUES["main_table_schema"])
        )
        
        # Default value for main_table_name if not already set
        default_main_table_name = ""
        if st.session_state.get("src_table_name") and not st.session_state.get("main_table_name"):
            base_table_name = st.session_state.get("src_table_name")
            if "_" in base_table_name:
                base_table_name = base_table_name.split("_", 1)[1] if base_table_name.count("_") > 0 else base_table_name
            default_main_table_name = f"DIM_{base_table_name}"
        else:
            default_main_table_name = st.session_state.get("main_table_name", "")
        
        main_table_name = st.text_input(
            "Main Table Name",
            value=default_main_table_name,
            help="Name of the main dimension table (e.g., DIM_Customer)"
        )
        
        # Sample columns for the main table
        st.text("Enter placeholder for domain-specific columns:")
        main_table_columns = st.text_area(
            "Table Columns (column name, data type)", 
            st.session_state.get("main_table_columns", """PARTY_ID bigint NULL,
RECORD_VERSION int NULL,
...... nvarchar(255) NULL"""), 
            help="Format: COLUMN_NAME DATA_TYPE NULL/NOT NULL, one per line. Use ...... as placeholder for additional columns."
        )
    
    # Add create helper table checkbox
    create_helper_table = st.checkbox(
        "Create helper table", 
        help="Creates a helper table with business key mapping",
        value=st.session_state.get("create_helper_table", False)
    )
    
    helper_schema = ""
    business_key_column = ""
    if create_helper_table:
        helper_schema = st.text_input(
            "Helper Table Schema", 
            st.session_state.get("helper_schema", DEFAULT_VALUES["helper_schema"])
        )
        business_key_column = st.text_input(
            "Business Key Column Name", 
            help="The main business key column to include in the helper table",
            value=st.session_state.get("business_key_column", "")
        )
    
    return (create_main_table, main_table_schema, main_table_name, main_table_columns,
            create_helper_table, helper_schema, business_key_column)

def render_delete_section():
    """Render the delete configuration section"""
    st.subheader("Delete Configuration")
    
    default_delete_type = st.session_state.get("delete_type", None)
    delete_type_index = DELETE_TYPE_OPTIONS.index(default_delete_type) if default_delete_type in DELETE_TYPE_OPTIONS else 0
    
    delete_type = st.selectbox(
        "Delete Type", 
        DELETE_TYPE_OPTIONS,
        index=delete_type_index,
        help="SOFT: Source table has a deleted flag\nHARD: Full comparison of business keys"
    )
    
    src_delete_column = None
    src_delete_value = None
    if delete_type == "SOFT":
        src_delete_column = st.text_input(
            "Source Delete Column", 
            st.session_state.get("src_delete_column", DEFAULT_VALUES["src_delete_column"]), 
            help="Source column used to mark records as deleted"
        )
        src_delete_value = st.text_input(
            "Source Delete Value", 
            st.session_state.get("src_delete_value", DEFAULT_VALUES["src_delete_value"]), 
            help="Value to mark records as deleted"
        )
    
    return delete_type, src_delete_column, src_delete_value

def render_advanced_options():
    """Render the advanced options section"""
    with st.expander("Advanced Options"):
        # Prescripts and Postscripts
        prescript = st.text_area(
            "Prescript SQL", 
            st.session_state.get("prescript", ""), 
            help="SQL to execute before loading data"
        )
        postscript = st.text_area(
            "Postscript SQL", 
            st.session_state.get("postscript", ""), 
            help="SQL to execute after loading data"
        )
        
        # Partitioning
        partitions = st.number_input(
            "Partitions", 1, 100, 
            st.session_state.get("partitions", DEFAULT_VALUES["partitions"]), 
            help="Number of partitions for SCD2 checks"
        )
        
        # Only show source column options when SCD2 from CT is selected
        use_source_column_for_valid_dates = False
        source_column_for_valid_from_date = ""
        source_column_for_sorting = ""
        
        if st.session_state.get("scd_type") == "SCD2 from CT":
            # Source Column for Valid Dates
            use_source_column_for_valid_dates = st.checkbox(
                "Use Source Column for Valid Dates", 
                st.session_state.get("use_source_column_for_valid_dates", DEFAULT_VALUES["use_source_column_for_valid_dates"])
            )
            
            if use_source_column_for_valid_dates:
                source_column_for_valid_from_date = st.text_input(
                    "Source Column for Valid From Date", 
                    st.session_state.get("source_column_for_valid_from_date", DEFAULT_VALUES["source_column_for_valid_from_date"]), 
                    help="Source column used to determine valid from date"
                )
            
            # Source Column for Sorting
            source_column_for_sorting = st.text_input(
                "Source Column for Sorting", 
                st.session_state.get("source_column_for_sorting", DEFAULT_VALUES["source_column_for_sorting"]), 
                help="Column for sorting incoming changes"
            )
        else:
            # Set these values in session state to ensure they're properly set to defaults when not SCD2 from CT
            st.session_state["use_source_column_for_valid_dates"] = False
            st.session_state["source_column_for_valid_from_date"] = ""
            st.session_state["source_column_for_sorting"] = ""
        
        return (prescript, postscript, partitions, use_source_column_for_valid_dates,
                source_column_for_valid_from_date, source_column_for_sorting)

def render_sidebar():
    """Render the complete sidebar"""
    # Render import parameters section first
    render_import_export_section()
    
    # Table Information header
    st.header("Table Information")
    
    # User Initials below the header
    user_initials = st.text_input("Your Initials (e.g., skg)", 
                                 value=st.session_state.get("user_initials", "")).lower()
    st.session_state.user_initials = user_initials
    
    # Render all other sections
    source_system_initial, source_system_daily, src_schema_name, src_table_name, src_table_name_ct = render_source_table_section()
    tgt_schema_name_st, tgt_table_name_st, tgt_schema_name_hs, tgt_table_name_hs = render_target_table_section(src_table_name)
    business_key, primary_key = render_key_columns_section()
    incremental_filter_st, incremental_filter_hs, incremental_filter_timezone = render_incremental_load_section()
    scd_type, scd2_columns_option, scd2_columns = render_scd_section()
    delete_type, src_delete_column, src_delete_value = render_delete_section()
    prescript, postscript, partitions, use_source_column_for_valid_dates, source_column_for_valid_from_date, source_column_for_sorting = render_advanced_options()
    create_main_table, main_table_schema, main_table_name, main_table_columns, create_helper_table, helper_schema, business_key_column = render_dimension_helper_section()
    
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
        "main_table_name": main_table_name,
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
        "skip_st_table": False,
        "skip_hs_table": False,
        "skip_main_table": False
    })
    
    # Generate SQL button in sidebar
    if st.button("Generate SQL Script", type="primary"):
        if not src_table_name or not business_key or (scd2_columns_option == "Specify Columns" and not scd2_columns):
            st.error("Please fill in all required fields: Source Table Name, Business Key, and SCD2 Columns")
        elif not user_initials:
            st.error("Please enter your initials")
        else:
            # Generate unique timestamp for table names - include both date and time for uniqueness
            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.session_state.timestamp = current_datetime
            st.session_state.table_suffix = f"{user_initials}_{current_datetime}"
            st.session_state.sql_generated = True
            st.success("SQL Generated Successfully! Check the tabs below.") 