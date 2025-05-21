import json
from datetime import datetime
import streamlit as st

def export_parameters(params):
    """Export the parameters to a JSON string"""
    try:
        # Extract specific parameters that we want to include
        export_params = {}
        for key in [
            "user_initials", 
            "source_system_initial", "source_system_daily", 
            "src_schema_name", "src_table_name", 
            "tgt_schema_name_st", "tgt_table_name_st", 
            "tgt_schema_name_hs", "tgt_table_name_hs", 
            "business_key", "primary_key", 
            "incremental_filter_st", "incremental_filter_hs", "incremental_filter_timezone", 
            "scd_type", "scd2_columns_option", "scd2_columns", 
            "delete_type", "src_delete_column", "src_delete_value", 
            "prescript", "postscript", "partitions", 
            "use_source_column_for_valid_dates", "source_column_for_valid_from_date", 
            "source_column_for_sorting", 
            "create_main_table", "main_table_schema", "main_table_name", "main_table_columns", 
            "create_helper_table", "helper_schema", "business_key_column"
        ]:
            if key in params and params[key] is not None:
                export_params[key] = params[key]
        
        # Add timestamp and app version
        export_params["export_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        export_params["app_version"] = "1.0.0"
        
        # Return the JSON string
        return json.dumps(export_params, indent=4)
    except Exception as e:
        raise Exception(f"Error exporting parameters: {str(e)}")

def import_parameters(json_string):
    """Import parameters from a JSON string"""
    try:
        # Load JSON
        params = json.loads(json_string)
        
        # Extract specific parameters that we want to include
        import_params = {}
        for key in [
            "user_initials", 
            "source_system_initial", "source_system_daily", 
            "src_schema_name", "src_table_name", 
            "tgt_schema_name_st", "tgt_table_name_st", 
            "tgt_schema_name_hs", "tgt_table_name_hs", 
            "business_key", "primary_key", 
            "incremental_filter_st", "incremental_filter_hs", "incremental_filter_timezone", 
            "scd_type", "scd2_columns_option", "scd2_columns", 
            "delete_type", "src_delete_column", "src_delete_value", 
            "prescript", "postscript", "partitions", 
            "use_source_column_for_valid_dates", "source_column_for_valid_from_date", 
            "source_column_for_sorting", 
            "create_main_table", "main_table_schema", "main_table_name", "main_table_columns", 
            "create_helper_table", "helper_schema", "business_key_column"
        ]:
            if key in params:
                import_params[key] = params[key]
        
        return import_params
    except Exception as e:
        raise Exception(f"Error importing parameters: {str(e)}")

def get_current_params():
    """Get the current parameters from session state"""
    # Extract specific parameters that we want to include
    current_params = {}
    for key in [
        "user_initials", 
        "source_system_initial", "source_system_daily", 
        "src_schema_name", "src_table_name", 
        "tgt_schema_name_st", "tgt_table_name_st", 
        "tgt_schema_name_hs", "tgt_table_name_hs", 
        "business_key", "primary_key", 
        "incremental_filter_st", "incremental_filter_hs", "incremental_filter_timezone", 
        "scd_type", "scd2_columns_option", "scd2_columns", 
        "delete_type", "src_delete_column", "src_delete_value", 
        "prescript", "postscript", "partitions", 
        "use_source_column_for_valid_dates", "source_column_for_valid_from_date", 
        "source_column_for_sorting", 
        "create_main_table", "main_table_schema", "main_table_name", "main_table_columns", 
        "create_helper_table", "helper_schema", "business_key_column"
    ]:
        if key in st.session_state:
            current_params[key] = st.session_state[key]
    
    return current_params 