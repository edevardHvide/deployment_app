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
        st.session_state.source_column_for_sorting,
        st.session_state.source_system_initial
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
        st.session_state.tgt_schema_name_st,
        st.session_state.tgt_table_name_st
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
    adf_json_initial = generate_adf_pipeline_json(
        st.session_state.src_table_name, 
        st.session_state.table_suffix, 
        True, 
        False, 
        False, 
        st.session_state.source_system_initial, 
        st.session_state.source_system_daily
    )
    adf_json_daily = generate_adf_pipeline_json(
        st.session_state.src_table_name, 
        st.session_state.table_suffix, 
        False, 
        False, 
        False, 
        st.session_state.source_system_initial, 
        st.session_state.source_system_daily
    )
    adf_json_invalid_hs = generate_adf_pipeline_json(
        st.session_state.src_table_name, 
        st.session_state.table_suffix, 
        True, 
        True, 
        False, 
        st.session_state.source_system_initial, 
        st.session_state.source_system_daily
    )
    
    # Convert the Python dictionaries to formatted JSON strings
    adf_json_str_initial = json.dumps(adf_json_initial, indent=4)
    adf_json_str_daily = json.dumps(adf_json_daily, indent=4)
    adf_json_str_invalid_hs = json.dumps(adf_json_invalid_hs, indent=4)
    
    # Create tabs for initial, daily, and invalid HS load JSONs
    initial_tab, invalid_hs_tab, daily_tab = st.tabs([
        "Initial Load Pipeline", 
        "Invalid HS Pipeline (Stage-only)", 
        "Daily Load Pipeline"
    ])
    
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
    
    with invalid_hs_tab:
        st.markdown("### Invalid HS Pipeline (Stage-only)")
        st.markdown("""
        #### Purpose
        This pipeline intentionally uses an invalid HS job name to run only the Stage part of the job. It will fail at the HS stage, which is expected.
        
        #### Workflow
        1. Run this pipeline first to load data into the Stage table
        2. After this pipeline completes (and fails at the HS stage), create the HS table using the script from Step 5
        3. Then run either:
           - The full initial load pipeline with correct parameters, or
           - A pipeline using the ST_Placeholder job to only run the HS part
        
        This approach helps prevent the "Invalid object name" error by creating the HS table before the actual HS job runs.
        """)
        st.code(adf_json_str_invalid_hs, language="json")
        st.download_button(
            label="Download Invalid HS Pipeline JSON",
            data=adf_json_str_invalid_hs,
            file_name=f"{adf_json_invalid_hs['name']}.json",
            mime="application/json",
            key="download_adf_json_invalid_hs",
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
    2. For each pipeline:
       - Navigate to the appropriate folder ("Deployment and initial load" or "Scheduling")
       - Create a new pipeline
       - Rename it to match the pipeline name
       - Click the "Code" button in the top right corner
       - Delete all existing code in the editor
       - Paste the JSON code
       - Click "Apply"
       - Save the pipeline
    
    3. Recommended deployment sequence:
       - Run the Invalid HS Pipeline first (this loads data to ST and fails at HS, which is expected)
       - Run the HS table creation script (Step 5)
       - Either run the Initial Load Pipeline again, or use ST_Placeholder to complete the HS part only
    
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
    st.subheader("Step 10: Create Dimension and Helper Tables")
    
    # Helper Table Creation
    helper_table_sql = generate_helper_table_sql(
        st.session_state.create_helper_table,
        st.session_state.helper_schema,
        st.session_state.business_key_column,
        st.session_state.src_table_name,
        st.session_state.business_key
    )
    
    if helper_table_sql:
        st.code(helper_table_sql)
    else:
        st.info("Helper table creation was not selected. Check the 'Create helper table' option in the Dimension and Helper Tables section to generate the helper table SQL.")
    
    # Main Table Creation
    main_table_sql = generate_main_table_sql(
        st.session_state.create_main_table,
        st.session_state.main_table_schema,
        st.session_state.main_table_columns,
        st.session_state.src_table_name,
        st.session_state.main_table_name,
        st.session_state.business_key_column
    )
    
    if main_table_sql:
        st.code(main_table_sql)
    else:
        st.info("Main table creation was not selected. Check the 'Create main DIM table' option to generate the main table SQL.")

def render_cleanup_tab(table_suffix):
    """Render the cleanup tab"""
    st.subheader("Step 9: Cleanup")
    
    cleanup_sql = f"""-- Cleanup:

-- a. Add the stage job definition to DWH.CONTROL_TABLE_STAGE

/*
select * into sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix} from DWH.CONTROL_TABLE_STAGE;
*/
/*
drop table DWH.CONTROL_TABLE_STAGE;
*/
/*
with cte as (
select * from sandbox.temp_control_table_st_{table_suffix}
union all
select * from sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix} 
where job_name+'|'+source_system+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
      not in (select job_name+'|'+source_system+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name from sandbox.temp_control_table_st_{table_suffix})
)
select * into DWH.CONTROL_TABLE_STAGE from cte;
*/

-- b. Add the HS job definition to DWH.CONTROL_TABLE_HS

/*
select * into sandbox.CONTROL_TABLE_HS_backup_{table_suffix} from DWH.CONTROL_TABLE_HS;
*/
/*
drop table DWH.CONTROL_TABLE_HS;
*/
/*
with cte as (
select * from sandbox.temp_control_table_hs_{table_suffix}
union all
select * from sandbox.CONTROL_TABLE_HS_backup_{table_suffix} 
where job_name+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
      not in (select job_name+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name from sandbox.temp_control_table_hs_{table_suffix})
)
select * into DWH.CONTROL_TABLE_HS from cte;
*/

-- c. drop the temporary tables:

--drop table sandbox.temp_control_table_hs_{table_suffix};
--drop table sandbox.temp_control_table_st_{table_suffix};
--drop table sandbox.temp_control_table_job_{table_suffix};
--drop table sandbox.CONTROL_TABLE_HS_backup_{table_suffix};
--drop table sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix};

-- If using an existing job name, the job should now be executed as part of that job's schedule.
"""
    
    st.code(cleanup_sql)
    
    st.markdown("""
    **NOTE:** Uncomment the statements in order when you are ready to move the configuration to production:
    1. First backup the existing control tables
    2. Drop the existing tables
    3. Create new tables with combined definitions
    4. Drop temporary tables when everything is verified
    """)

def render_main_content():
    """Render the main content area with all tabs"""
    st.header("Generated SQL Deployment Script")
    
    # Show table suffix information if SQL is generated
    if st.session_state.sql_generated:
        table_suffix = st.session_state.table_suffix
        # Check if this looks like a fresh generation (contains current user's initials and recent timestamp)
        user_initials = st.session_state.get('user_initials', '').lower()
        if user_initials and table_suffix.startswith(f"{user_initials}_"):
            st.info(f"📋 **Table Suffix:** `{table_suffix}` - Generated for this session. This suffix will be preserved when you export parameters for deployment.")
        else:
            st.info(f"📋 **Table Suffix:** `{table_suffix}` - Imported from configuration. This ensures consistency with the original SQL generation.")
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
        "1. Control Tables Backup", 
        "2. ST Control Table", 
        "3. HS Control Table", 
        "4. Job Control Table",
        "5. Create HS Table",
        "6. ADF Pipeline JSON",
        "7. Verify Deployment",
        "8. Cleanup",
        "9. Dimension and Helper Tables"
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
            st.subheader("Step 8: Verify Deployment")
            st.markdown(f"""
            Run the following queries to verify your deployment:
            
            ```sql
            -- Verify ST table has data
            SELECT TOP 10 * FROM {st.session_state.tgt_schema_name_st}.{st.session_state.tgt_table_name_st};
            
            -- Verify HS table structure and data
            SELECT TOP 10 * FROM {st.session_state.tgt_schema_name_hs}.{st.session_state.tgt_table_name_hs};
            
            -- Check technical columns in HS table
            SELECT TC_CURRENT_FLAG, TC_VALID_FROM_DATE, TC_VALID_TO_DATE, TC_CHECKSUM_BUSKEY, TC_CHECKSUM_SCD 
            FROM {st.session_state.tgt_schema_name_hs}.{st.session_state.tgt_table_name_hs} 
            WHERE TC_CURRENT_FLAG = 'Y';
            ```
            """)
        
        with tab8:
            render_cleanup_tab(table_suffix)
        
        with tab9:
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
    st.session_state.source_column_for_sorting,
    st.session_state.source_system_initial
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
    st.session_state.tgt_schema_name_st,
    st.session_state.tgt_table_name_st
)}

---------------------------------------------------------
-- STEP 8: VERIFY DEPLOYMENT
---------------------------------------------------------
-- Verify ST table has data
-- SELECT TOP 10 * FROM {st.session_state.tgt_schema_name_st}.{st.session_state.tgt_table_name_st};

-- Verify HS table structure and data
-- SELECT TOP 10 * FROM {st.session_state.tgt_schema_name_hs}.{st.session_state.tgt_table_name_hs};

-- Check technical columns in HS table
-- SELECT TC_CURRENT_FLAG, TC_VALID_FROM_DATE, TC_VALID_TO_DATE, TC_CHECKSUM_BUSKEY, TC_CHECKSUM_SCD 
-- FROM {st.session_state.tgt_schema_name_hs}.{st.session_state.tgt_table_name_hs} 
-- WHERE TC_CURRENT_FLAG = 'Y';

---------------------------------------------------------
-- STEP 9: CLEANUP
---------------------------------------------------------
-- Cleanup:

-- a. Add the stage job definition to DWH.CONTROL_TABLE_STAGE

/*
select * into sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix} from DWH.CONTROL_TABLE_STAGE;
*/
/*
drop table DWH.CONTROL_TABLE_STAGE;
*/
/*
with cte as (
select * from sandbox.temp_control_table_st_{table_suffix}
union all
select * from sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix} 
where job_name+'|'+source_system+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
      not in (select job_name+'|'+source_system+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name from sandbox.temp_control_table_st_{table_suffix})
)
select * into DWH.CONTROL_TABLE_STAGE from cte;
*/

-- b. Add the HS job definition to DWH.CONTROL_TABLE_HS

/*
select * into sandbox.CONTROL_TABLE_HS_backup_{table_suffix} from DWH.CONTROL_TABLE_HS;
*/
/*
drop table DWH.CONTROL_TABLE_HS;
*/
/*
with cte as (
select * from sandbox.temp_control_table_hs_{table_suffix}
union all
select * from sandbox.CONTROL_TABLE_HS_backup_{table_suffix} 
where job_name+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name 
      not in (select job_name+'|'+src_schema_name+'|'+src_table_name+'|'+tgt_schema_name+'|'+tgt_table_name from sandbox.temp_control_table_hs_{table_suffix})
)
select * into DWH.CONTROL_TABLE_HS from cte;
*/

-- c. drop the temporary tables:

--drop table sandbox.temp_control_table_hs_{table_suffix};
--drop table sandbox.temp_control_table_st_{table_suffix};
--drop table sandbox.temp_control_table_job_{table_suffix};
--drop table sandbox.CONTROL_TABLE_HS_backup_{table_suffix};
--drop table sandbox.CONTROL_TABLE_STAGE_backup_{table_suffix};

---------------------------------------------------------
-- STEP 10: CREATE HELPER AND DIMENSION TABLES
---------------------------------------------------------
{generate_helper_table_sql(
    st.session_state.create_helper_table,
    st.session_state.helper_schema,
    st.session_state.business_key_column,
    st.session_state.src_table_name,
    st.session_state.business_key
) or ""}

{generate_main_table_sql(
    st.session_state.create_main_table,
    st.session_state.main_table_schema,
    st.session_state.main_table_columns,
    st.session_state.src_table_name,
    st.session_state.main_table_name,
    st.session_state.business_key_column
) or ""}
-- End of script
"""
        
        # Store the complete SQL in session state
        st.session_state.all_sql = complete_sql
        
        # Create a download button for the complete SQL script
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
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
        
        with col2:
            st.subheader("Export Parameters")
            st.info(f"💾 **Table Suffix Preservation**: The current table suffix `{table_suffix}` will be saved in the exported configuration to ensure deployment consistency.")
            
            # Create a unique file name for the parameters
            if st.session_state.src_table_name:
                file_name = f"dwh_params_{st.session_state.src_table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            else:
                file_name = f"dwh_params_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Prepare the parameters to export
            from src.utils.parameters import export_parameters
            params_to_export = {}
            
            # Get all relevant parameters from session state
            for key in [
                "user_initials", "table_suffix",
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
                "create_main_table", "main_table_schema", 
                "main_table_columns", "skip_st_table", "skip_hs_table", "skip_main_table"
            ]:
                # Include parameter if it exists in session state
                if key in st.session_state:
                    params_to_export[key] = st.session_state[key]
                else:
                    params_to_export[key] = None
            
            # Export parameters to JSON
            params_json = export_parameters(params_to_export)
            
            # Direct download button
            download_button = st.download_button(
                label="Export Current Parameters",
                data=params_json,
                file_name=file_name,
                mime="application/json",
                key="export_params"
            )
    else:
        st.info("Fill in the required fields in the sidebar and click 'Generate SQL Script' to see the deployment steps.") 