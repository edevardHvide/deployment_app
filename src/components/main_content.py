import streamlit as st
from datetime import datetime
import json
from src.utils.sql_generator import (
    generate_control_table_backup_sql,
    generate_st_control_table_sql,
    generate_hs_control_table_sql,
    generate_job_control_sql,
    generate_hs_table_sql,
    generate_helper_table_sql,
    generate_main_table_sql
)
from src.utils.adf_generator import generate_adf_pipeline_json
import io

def render_control_table_backup_tab(table_suffix):
    """Render the control table backup tab"""
    st.subheader("Step 1: Create Temporary Control Tables")
    tab1_sql = generate_control_table_backup_sql(table_suffix)
    st.code(tab1_sql)

def render_st_control_table_tab(table_suffix):
    """Render the ST control table tab"""
    st.subheader("Step 2: Update ST Control Table")
    tab2_sql = generate_st_control_table_sql(
        table_suffix,
        st.session_state.source_system_initial,
        st.session_state.source_system_daily,
        st.session_state.src_schema_name,
        st.session_state.src_table_name,
        st.session_state.src_table_name_ct,
        st.session_state.tgt_schema_name_st,
        st.session_state.tgt_table_name_st,
        st.session_state.business_key,
        st.session_state.incremental_filter_st,
        st.session_state.incremental_filter_timezone,
        st.session_state.delete_type,
        st.session_state.src_delete_column,
        st.session_state.src_delete_value
    )
    st.code(tab2_sql)

def render_hs_control_table_tab(table_suffix):
    """Render the HS control table tab"""
    st.subheader("Step 3: Update HS Control Table")
    tab3_sql = generate_hs_control_table_sql(
        table_suffix,
        st.session_state.tgt_schema_name_st,
        st.session_state.tgt_table_name_st,
        st.session_state.tgt_schema_name_hs,
        st.session_state.tgt_table_name_hs,
        st.session_state.business_key,
        st.session_state.primary_key,
        st.session_state.incremental_filter_hs,
        st.session_state.incremental_filter_timezone,
        st.session_state.scd_type,
        st.session_state.scd2_columns,
        st.session_state.prescript,
        st.session_state.postscript,
        st.session_state.partitions,
        st.session_state.use_source_column_for_valid_dates,
        st.session_state.source_column_for_valid_from_date,
        st.session_state.source_column_for_sorting
    )
    st.code(tab3_sql)

def render_job_control_tab(table_suffix):
    """Render the job control table tab"""
    st.subheader("Step 4: Update Job Control Table")
    tab4_sql = generate_job_control_sql(table_suffix)
    st.code(tab4_sql)
    
    st.markdown("""
    In the next step, we will create the ADF pipeline JSON that will use these control table configurations to orchestrate the data loading process.
    """)

def render_hs_table_tab():
    """Render the HS table creation tab"""
    st.subheader("Step 5: Create HS Table")
    tab5_sql = generate_hs_table_sql(
        st.session_state.tgt_schema_name_hs,
        st.session_state.tgt_table_name_hs,
        st.session_state.skip_hs_table
    )
    st.code(tab5_sql)
    
    st.markdown("""
    **Note:** If you want a quicker way to get to the HS tables, you can run the initial load with an invalid HS job name. 
    This will only run the stage part of the job and then fail. Then create the HS table with this script.
    """)

def render_adf_pipeline_tab():
    """Render the ADF pipeline JSON tab"""
    st.subheader("Step 6: ADF Pipeline JSON")
    
    # Generate the ADF pipeline JSON for both initial and daily loads
    adf_json_initial = generate_adf_pipeline_json(st.session_state.src_table_name, st.session_state.table_suffix, True)
    adf_json_daily = generate_adf_pipeline_json(st.session_state.src_table_name, st.session_state.table_suffix, False)
    
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
            file_name=f"{adf_json_initial['name']}.json",
            mime="application/json",
            key="download_adf_json_initial",
        )
    
    with daily_tab:
        st.markdown("### Daily Load Pipeline")
        st.code(adf_json_str_daily, language="json")
        st.download_button(
            label="Download Daily Load Pipeline JSON",
            data=adf_json_str_daily,
            file_name=f"{adf_json_daily['name']}.json",
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

def render_dimension_helper_tab():
    """Render the dimension and helper tables tab"""
    st.subheader("Step 7: Create Dimension and Helper Tables")
    
    # Helper Table Creation
    helper_table_sql = generate_helper_table_sql(
        st.session_state.create_helper_table,
        st.session_state.helper_schema,
        st.session_state.business_key_column,
        st.session_state.src_table_name
    )
    
    if helper_table_sql:
        st.code(helper_table_sql)
    else:
        st.info("Helper table creation was not selected. Check the 'Create helper table' option in the Dimension and Helper Tables section to generate the helper table SQL.")
    
    # Main Table Creation
    main_table_sql = generate_main_table_sql(
        st.session_state.create_main_table,
        st.session_state.skip_main_table,
        st.session_state.main_table_schema,
        st.session_state.main_table_prefix,
        st.session_state.main_table_columns,
        st.session_state.src_table_name
    )
    
    if main_table_sql:
        st.code(main_table_sql)
    else:
        if st.session_state.skip_main_table:
            st.info("Main table creation skipped as per user selection.")
        else:
            st.info("Main table creation was not selected. Check the 'Create main DIM table' option to generate the main table SQL.")

def render_main_content():
    """Render the main content area with all tabs"""
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
        
        with tab1:
            render_control_table_backup_tab(table_suffix)
        
        with tab2:
            render_st_control_table_tab(table_suffix)
        
        with tab3:
            render_hs_control_table_tab(table_suffix)
        
        with tab4:
            render_job_control_tab(table_suffix)
        
        with tab5:
            render_hs_table_tab()
        
        with tab6:
            render_adf_pipeline_tab()
        
        with tab7:
            render_dimension_helper_tab()
        
        # Prepare complete SQL script for download
        complete_sql = f"""-- Generated SQL Deployment Script for {st.session_state.src_table_name}
-- Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- This script contains all steps needed for deploying {st.session_state.src_table_name} to the data warehouse.
-- Created by: {st.session_state.user_initials.upper()}

---------------------------------------------------------
-- STEP 1: CREATE TEMPORARY CONTROL TABLES
---------------------------------------------------------
{generate_control_table_backup_sql(table_suffix)}

---------------------------------------------------------
-- STEP 2: UPDATE ST CONTROL TABLE
---------------------------------------------------------
{generate_st_control_table_sql(
    table_suffix,
    st.session_state.source_system_initial,
    st.session_state.source_system_daily,
    st.session_state.src_schema_name,
    st.session_state.src_table_name,
    st.session_state.src_table_name_ct,
    st.session_state.tgt_schema_name_st,
    st.session_state.tgt_table_name_st,
    st.session_state.business_key,
    st.session_state.incremental_filter_st,
    st.session_state.incremental_filter_timezone,
    st.session_state.delete_type,
    st.session_state.src_delete_column,
    st.session_state.src_delete_value
)}

---------------------------------------------------------
-- STEP 3: UPDATE HS CONTROL TABLE
---------------------------------------------------------
{generate_hs_control_table_sql(
    table_suffix,
    st.session_state.tgt_schema_name_st,
    st.session_state.tgt_table_name_st,
    st.session_state.tgt_schema_name_hs,
    st.session_state.tgt_table_name_hs,
    st.session_state.business_key,
    st.session_state.primary_key,
    st.session_state.incremental_filter_hs,
    st.session_state.incremental_filter_timezone,
    st.session_state.scd_type,
    st.session_state.scd2_columns,
    st.session_state.prescript,
    st.session_state.postscript,
    st.session_state.partitions,
    st.session_state.use_source_column_for_valid_dates,
    st.session_state.source_column_for_valid_from_date,
    st.session_state.source_column_for_sorting
)}

---------------------------------------------------------
-- STEP 4: UPDATE JOB CONTROL TABLE
---------------------------------------------------------
{generate_job_control_sql(table_suffix)}

---------------------------------------------------------
-- STEP 5: CREATE HS TABLE
---------------------------------------------------------
{generate_hs_table_sql(
    st.session_state.tgt_schema_name_hs,
    st.session_state.tgt_table_name_hs,
    st.session_state.skip_hs_table
)}

---------------------------------------------------------
-- STEP 6: ADF PIPELINE JSON
---------------------------------------------------------
{generate_helper_table_sql(
    st.session_state.create_helper_table,
    st.session_state.helper_schema,
    st.session_state.business_key_column,
    st.session_state.src_table_name
) or ""}

{generate_main_table_sql(
    st.session_state.create_main_table,
    st.session_state.skip_main_table,
    st.session_state.main_table_schema,
    st.session_state.main_table_prefix,
    st.session_state.main_table_columns,
    st.session_state.src_table_name
) or ""}
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
            file_name=f"deploy_{st.session_state.src_table_name}_{table_suffix}.sql",
            mime="text/plain",
            key="download_sql",
        )
    else:
        st.info("Fill in the required fields in the sidebar and click 'Generate SQL Script' to see the deployment steps.") 