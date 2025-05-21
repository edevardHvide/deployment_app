import json
from datetime import datetime
import streamlit as st

def export_parameters(params):
    """Export all user input parameters to a JSON file"""
    # Make sure all parameters are included, even if they're None
    all_params = {}
    
    # List of all expected parameters
    expected_params = [
        "user_initials", 
        "source_system_initial", "source_system_daily",
        "src_schema_name", "src_table_name", "src_table_name_ct",
        "tgt_schema_name_st", "tgt_table_name_st", 
        "tgt_schema_name_hs", "tgt_table_name_hs",
        "business_key", "primary_key",
        "incremental_filter_st", "incremental_filter_hs", "incremental_filter_timezone",
        "scd_type", "scd2_columns_option", "scd2_columns",
        "create_helper_table", "helper_schema", "business_key_column",
        "delete_type", "src_delete_column", "src_delete_value",
        "prescript", "postscript", "partitions", 
        "use_source_column_for_valid_dates", "source_column_for_valid_from_date", 
        "source_column_for_sorting",
        "create_main_table", "main_table_schema", "main_table_prefix", 
        "main_table_columns", "skip_st_table", "skip_hs_table", "skip_main_table"
    ]
    
    # Set default values for all expected parameters
    for param in expected_params:
        all_params[param] = None
    
    # Update with provided values
    all_params.update(params)
    
    # Add metadata
    all_params["export_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_params["app_version"] = "1.0.0"
    
    # Convert to JSON
    params_json = json.dumps(all_params, indent=4)
    return params_json

def import_parameters(json_str):
    """Import parameters from a JSON string and return a dictionary of values"""
    try:
        params = json.loads(json_str)
        # Validate that essential parameters are present
        essential_params = ["user_initials", "src_schema_name"]
        for param in essential_params:
            if param not in params:
                st.warning(f"Missing essential parameter: {param}")
        return params
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON format: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Error importing parameters: {str(e)}")
        return None

def get_current_params(user_initials, source_system_initial, source_system_daily, 
                      src_schema_name, src_table_name, tgt_schema_name_st, 
                      tgt_table_name_st, tgt_schema_name_hs, tgt_table_name_hs,
                      business_key, primary_key, incremental_filter_st, 
                      incremental_filter_hs, incremental_filter_timezone,
                      scd_type, scd2_columns_option, scd2_columns,
                      create_helper_table, helper_schema, business_key_column,
                      delete_type, src_delete_column, src_delete_value,
                      prescript, postscript, partitions,
                      use_source_column_for_valid_dates,
                      source_column_for_valid_from_date,
                      source_column_for_sorting,
                      create_main_table, main_table_schema,
                      main_table_prefix, main_table_columns,
                      skip_st_table, skip_hs_table, skip_main_table):
    """Get all current parameter values for export"""
    return {
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
        "scd2_columns": scd2_columns,
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
        "create_main_table": create_main_table,
        "main_table_schema": main_table_schema,
        "main_table_prefix": main_table_prefix,
        "main_table_columns": main_table_columns,
        "skip_st_table": skip_st_table,
        "skip_hs_table": skip_hs_table,
        "skip_main_table": skip_main_table
    } 