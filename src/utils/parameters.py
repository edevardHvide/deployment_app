import json
from datetime import datetime
import streamlit as st

def export_parameters(params):
    """Export all user input parameters to a JSON file"""
    params_json = json.dumps(params, indent=4)
    return params_json

def import_parameters(json_str):
    """Import parameters from a JSON string and return a dictionary of values"""
    try:
        params = json.loads(json_str)
        return params
    except json.JSONDecodeError:
        st.error("Invalid JSON format")
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
        "src_table_name": src_table_name if src_table_name else None,
        "tgt_schema_name_st": tgt_schema_name_st,
        "tgt_table_name_st": tgt_table_name_st if tgt_table_name_st else None,
        "tgt_schema_name_hs": tgt_schema_name_hs,
        "tgt_table_name_hs": tgt_table_name_hs if tgt_table_name_hs else None,
        "business_key": business_key if business_key else None,
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