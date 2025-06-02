import streamlit as st
from datetime import datetime
import json
from src.utils.parameters import import_parameters
from src.utils.sql_generator import (
    generate_control_table_backup_sql,
    generate_st_control_table_sql,
    generate_hs_control_table_sql,
    generate_job_control_sql,
    generate_hs_table_sql,
    generate_helper_table_sql,
    generate_main_table_sql
)

def render_deployer_sidebar():
    """Render the simplified sidebar for deployers"""
    st.title("Deployment App")
    st.subheader("Deployer View")
    
    st.markdown("---")
    
    # Switch back to role selection
    if st.button("Change Role", use_container_width=True):
        st.session_state.user_role = None
        st.rerun()
    
    st.markdown("---")
    
    # Import parameters section
    st.subheader("Import Configuration")
    st.markdown("Upload the JSON configuration file provided by the developer:")
    
    uploaded_file = st.file_uploader("Upload Configuration File", type=['json'])
    if uploaded_file is not None:
        params_str = uploaded_file.getvalue().decode()
        try:
            imported_params = import_parameters(params_str)
            if imported_params:
                st.success("Configuration loaded successfully! Click 'Apply Configuration' to proceed.")
                if st.button("Apply Configuration", use_container_width=True):
                    # Store all imported parameters directly in session state
                    for key, value in imported_params.items():
                        if key not in ["export_timestamp", "app_version"]:  # Skip metadata
                            st.session_state[key] = value
                    
                    # Calculate the CT table name if needed
                    if "src_table_name" in st.session_state and "source_system_daily" in st.session_state:
                        if st.session_state.src_table_name and st.session_state.source_system_daily == "Replicate_CDC":
                            st.session_state.src_table_name_ct = f"{st.session_state.src_table_name}__ct"
                        else:
                            st.session_state.src_table_name_ct = st.session_state.src_table_name
                    
                    # Set the SQL generation flag to true to display deployment steps
                    st.session_state.sql_generated = True
                    
                    # Set a table suffix if not present
                    if 'table_suffix' not in st.session_state:
                        st.session_state.table_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    st.success("Configuration applied successfully!")
                    st.rerun()
        except Exception as e:
            st.error(f"Error importing configuration: {str(e)}")

def render_deployment_instructions():
    """Render the deployment instructions for deployers"""
    st.header("Deployment Instructions")
    
    if not st.session_state.sql_generated:
        st.info("Please upload and apply a configuration file to view deployment instructions.")
        return
    
    # Ensure src_table_name_ct is initialized (fallback protection)
    if "src_table_name_ct" not in st.session_state and "src_table_name" in st.session_state:
        if "source_system_daily" in st.session_state and st.session_state.source_system_daily == "Replicate_CDC":
            st.session_state.src_table_name_ct = f"{st.session_state.src_table_name}__ct"
        else:
            st.session_state.src_table_name_ct = st.session_state.src_table_name
    
    # Ensure other required state variables have defaults
    required_state_vars = [
        "source_system_initial", "source_system_daily", 
        "src_schema_name", "src_table_name", "src_table_name_ct",
        "tgt_schema_name_st", "tgt_table_name_st", 
        "tgt_schema_name_hs", "tgt_table_name_hs",
        "business_key", "primary_key", "incremental_filter_st", 
        "incremental_filter_hs", "incremental_filter_timezone",
        "scd_type", "scd2_columns", "delete_type",
        "src_delete_column", "src_delete_value"
    ]
    
    # Check if any required variables are missing and set defaults from src.config.constants
    from src.config.constants import DEFAULT_VALUES
    for var in required_state_vars:
        if var not in st.session_state:
            if var in DEFAULT_VALUES:
                st.session_state[var] = DEFAULT_VALUES[var]
            elif var == "src_table_name_ct" and "src_table_name" in st.session_state:
                st.session_state[var] = st.session_state.src_table_name
            elif var in ["prescript", "postscript", "scd2_columns"]:
                st.session_state[var] = ""
            elif var == "delete_type":
                st.session_state[var] = None
            elif var == "src_delete_column":
                st.session_state[var] = "DELETED_FLAG"
            elif var == "src_delete_value":
                st.session_state[var] = "Y"
    
    # Special handling for Profisee sources: Override the tgt_table_name_st with the exact case format
    if "source_system_initial" in st.session_state and "src_table_name" in st.session_state:
        if st.session_state.source_system_initial and "Profisee" in st.session_state.source_system_initial:
            # Ensure we're using Profisee_dev throughout
            st.session_state.source_system_initial = "Profisee_dev"
            if st.session_state.source_system_daily and "Profisee" in st.session_state.source_system_daily:
                st.session_state.source_system_daily = "Profisee_dev"
            
            # Always override to ensure consistency
            st.session_state.tgt_table_name_st = f"ST_PRO_{st.session_state.src_table_name}"
            st.session_state.tgt_table_name_hs = f"HS_PRO_{st.session_state.src_table_name}"
    
    # Get the table suffix
    table_suffix = st.session_state.table_suffix
    
    # Display table information
    st.markdown(f"""
    ## Deployment for: {st.session_state.src_table_name}
    
    **Configuration uploaded by:** {st.session_state.get('user_initials', 'Developer')}  
    **Table suffix:** `{table_suffix}` *(preserved from original configuration - ensures temp tables match)*  
    **Target tables:**
    - ST table: `{st.session_state.tgt_schema_name_st}.{st.session_state.tgt_table_name_st}`
    - HS table: `{st.session_state.tgt_schema_name_hs}.{st.session_state.tgt_table_name_hs}`
    
    **Temp control tables that will be created:**
    - `sandbox.temp_control_table_st_{table_suffix}`
    - `sandbox.temp_control_table_hs_{table_suffix}`
    - `sandbox.temp_control_table_job_{table_suffix}`
    """)
    
    # Create tabs for deployment steps
    deploy_tab1, deploy_tab3 = st.tabs([
        "Step-by-Step Deployment", 
        "Download Files"
    ])
    
    with deploy_tab1:
        st.subheader("Step-by-Step Deployment")
        
        try:
            # Final check to ensure Profisee table names are consistent
            if st.session_state.source_system_initial and "Profisee" in st.session_state.source_system_initial:
                # Ensure we're using Profisee_dev throughout
                st.session_state.source_system_initial = "Profisee_dev"
                if st.session_state.source_system_daily and "Profisee" in st.session_state.source_system_daily:
                    st.session_state.source_system_daily = "Profisee_dev"
                
                # Always override to ensure consistency
                st.session_state.tgt_table_name_st = f"ST_PRO_{st.session_state.src_table_name}"
                st.session_state.tgt_table_name_hs = f"HS_PRO_{st.session_state.src_table_name}"
            
            # Generate all SQL scripts
            backup_sql = generate_control_table_backup_sql(
                table_suffix,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            st_control_sql = generate_st_control_table_sql(
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
                st.session_state.get("delete_type", None),
                st.session_state.get("src_delete_column", "DELETED_FLAG"),
                st.session_state.get("src_delete_value", "Y")
            )
            hs_control_sql = generate_hs_control_table_sql(
                table_suffix,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily,
                st.session_state.src_schema_name,
                st.session_state.src_table_name,
                st.session_state.tgt_schema_name_st,
                st.session_state.tgt_schema_name_hs,
                st.session_state.tgt_table_name_hs,
                st.session_state.business_key,
                st.session_state.primary_key,
                st.session_state.incremental_filter_hs,
                st.session_state.incremental_filter_timezone,
                st.session_state.scd_type,
                st.session_state.scd2_columns,
                st.session_state.get("prescript", ""),
                st.session_state.get("postscript", ""),
                st.session_state.get("partitions", 1),
                st.session_state.get("use_source_column_for_valid_dates", False),
                st.session_state.get("source_column_for_valid_from_date", None),
                st.session_state.get("source_column_for_sorting", None)
            )
            job_control_sql = generate_job_control_sql(
                table_suffix,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            hs_table_sql = generate_hs_table_sql(
                st.session_state.tgt_schema_name_hs,
                st.session_state.tgt_table_name_hs,
                st.session_state.tgt_schema_name_st,
                st.session_state.tgt_table_name_st,
                st.session_state.source_system_initial,
                st.session_state.src_table_name
            )
            
            # Add quick HS table creation script
            from src.utils.sql_generator import generate_hs_table_quick_creation_sql
            hs_quick_creation_sql = generate_hs_table_quick_creation_sql(
                st.session_state.tgt_schema_name_hs,
                st.session_state.tgt_table_name_hs,
                st.session_state.tgt_schema_name_st,
                st.session_state.tgt_table_name_st,
                st.session_state.source_system_initial,
                st.session_state.src_table_name
            )
            
            # Helper & Main Table SQL (if configured)
            helper_table_sql = generate_helper_table_sql(
                st.session_state.get("create_helper_table", False),
                st.session_state.get("helper_schema", ""),
                st.session_state.get("business_key_column", ""),
                st.session_state.src_table_name,
                st.session_state.get("business_key", "")
            )
            
            main_table_sql = generate_main_table_sql(
                st.session_state.get("create_main_table", False),
                st.session_state.get("main_table_schema", ""),
                st.session_state.get("main_table_columns", ""),
                st.session_state.src_table_name,
                st.session_state.get("main_table_name", ""),
                st.session_state.get("business_key_column", "")
            )
            
            # Generate ADF pipeline JSON
            from src.utils.adf_generator import generate_adf_pipeline_json
            adf_json_invalid_hs = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                True, 
                True, 
                False,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            adf_json_str_invalid_hs = json.dumps(adf_json_invalid_hs, indent=4)
            
            # Generate the ADF pipeline JSON for all pipeline types
            adf_json_initial = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                True, 
                False, 
                False,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            adf_json_daily = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                False, 
                False, 
                False,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            adf_json_placeholder = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                True, 
                False, 
                True,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            
            # Convert the Python dictionaries to formatted JSON strings
            adf_json_str_initial = json.dumps(adf_json_initial, indent=4)
            adf_json_str_daily = json.dumps(adf_json_daily, indent=4)
            adf_json_str_placeholder = json.dumps(adf_json_placeholder, indent=4)
            
            # ------------------- MERGED STEP-BY-STEP INSTRUCTIONS AND SQL -------------------
            
            # STEP 1-4: Initial Control Table Setup
            st.markdown("### STEP 1-4: Initial Database Setup")
            st.markdown("""
            Run the following SQL script to set up the control tables for deployment:
            - Create temporary control tables
            - Configure the ST control table
            - Configure the HS control table
            - Set up the job control settings
            """)
            
            initial_setup_sql = f"""-- STEP 1: CREATE TEMPORARY CONTROL TABLES
{backup_sql}

-- STEP 2: UPDATE ST CONTROL TABLE
{st_control_sql}

-- STEP 3: UPDATE HS CONTROL TABLE
{hs_control_sql}

-- STEP 4: UPDATE JOB CONTROL TABLE
{job_control_sql}
"""
            st.code(initial_setup_sql, language="sql")
            
            # STEP 5: Run the Invalid HS Pipeline
            st.markdown("### STEP 5: Run Invalid HS Pipeline")
            st.markdown("""
            🚨 **ACTION REQUIRED**: Run the "Invalid HS Pipeline" in Azure Data Factory
            
            1. In Azure Data Factory, locate the pipeline named:
               `{}`
            2. This pipeline should already exist in your environment after branch deployment
            3. Run this pipeline to load data into the Stage table
            4. This pipeline will intentionally fail after the Stage part - this is expected
            """.format(adf_json_invalid_hs['name']))
            
            st.info("⏳ Wait for the pipeline to complete the Stage part and fail at the HS part before proceeding.")
            
            # STEP 6: Create the HS Table
            st.markdown("### STEP 6: Create HS Table")
            st.markdown("""
            Now that the Stage table has been populated, run the following SQL to create the HS table with all required technical columns:
            """)
            
            st.code(hs_table_sql, language="sql")
            
            # Add new Step - Update Job Control again
            st.markdown("### STEP 6.5: Re-update Job Control Table")
            st.markdown("""
            After creating the HS table, we need to re-run the job control SQL to ensure all jobs are properly set up:
            """)
            
            st.code(job_control_sql, language="sql")
            
            # STEP 7: Complete the Initial Load
            st.markdown("### STEP 7: Complete the Initial Load")
            st.markdown("""
            🚨 **ACTION REQUIRED**: Choose one of these options to complete the load:
            
            **Option A**: Run the HS-only pipeline with ST_Placeholder
            1. In Azure Data Factory, locate the pipeline named:
               `{}`
            2. This pipeline should already exist in your environment after branch deployment
            3. It uses ST_Placeholder to skip the Stage part and run only the HS part
            """.format(adf_json_placeholder['name']))
            
            # Generate SQL to update job control table for ST_Placeholder
            st_initial_job = "ST_Profisee_Initial" if st.session_state.source_system_initial and "Profisee_dev" in st.session_state.source_system_initial else "ST_Full_Initial"
            
            st_placeholder_sql = f"""-- Run this SQL if using Option A (ST_Placeholder)
-- This updates the job control table to use ST_Placeholder instead of the original job name
UPDATE sandbox.temp_control_table_job_{table_suffix}
SET job_name = 'ST_Placeholder'
WHERE job_name = '{st_initial_job}';
"""
            st.code(st_placeholder_sql, language="sql")
            
            st.markdown("""
            **Option B**: Run the original Initial Load pipeline again
            1. In Azure Data Factory, locate the pipeline named:
               `{}`
            2. This pipeline should already exist in your environment after branch deployment
            3. Run this pipeline to perform the full initial load using the now-created HS table
            """.format(adf_json_initial['name']))
            
            # STEP 9: Verification
            st.markdown("### STEP 9: Verify Deployment and Set Up Daily Load")
            
            # Determine actual table names for display based on source system
            display_st_table = st.session_state.tgt_table_name_st
            display_hs_table = st.session_state.tgt_table_name_hs
            
            st.markdown(f"""
            #### 9.1 Verify Tables
            Run the following queries to verify your deployment:
            
            ```sql
            -- Verify ST table has data
            SELECT TOP 10 * FROM {st.session_state.tgt_schema_name_st}.{display_st_table};
            
            -- Verify HS table structure and data
            SELECT TOP 10 * FROM {st.session_state.tgt_schema_name_hs}.{display_hs_table};
            
            -- Check technical columns in HS table
            SELECT TC_CURRENT_FLAG, TC_VALID_FROM_DATE, TC_VALID_TO_DATE, TC_CHECKSUM_BUSKEY, TC_CHECKSUM_SCD 
            FROM {st.session_state.tgt_schema_name_hs}.{display_hs_table} 
            WHERE TC_CURRENT_FLAG = 'Y';
            ```
            
            #### 9.2 Set Up Daily Load
            After successful initial load:
            
            1. In Azure Data Factory, locate the daily load pipeline named:
               `{adf_json_daily['name']}`
            2. This pipeline should already exist in your environment after branch deployment
            3. Set up an appropriate trigger schedule for this pipeline based on your requirements
            4. This pipeline will handle all future daily loads for this table
            """)
            
            # Add cleanup step
            st.markdown("### STEP 10: Cleanup")
            
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
"""
            
            st.code(cleanup_sql)
            
            st.markdown("""
            **NOTE:** Uncomment the statements in order when you are ready to move the configuration to production:
            1. First backup the existing control tables
            2. Drop the existing tables
            3. Create new tables with combined definitions
            4. Drop temporary tables when everything is verified
            
            If using an existing job name, the job should now be executed as part of that job's schedule.
            """)
            
            # STEP 8: Additional Tables (if any) - Moved to STEP 11
            if helper_table_sql or main_table_sql:
                st.markdown("### STEP 11: Create Additional Tables")
                st.markdown("""
                Run the following SQL to create any additional helper or dimension tables:
                """)
                
                additional_tables_sql = ""
                if helper_table_sql:
                    additional_tables_sql += f"""-- HELPER TABLE
{helper_table_sql}

"""
                if main_table_sql:
                    additional_tables_sql += f"""-- MAIN TABLE
{main_table_sql}
"""
                st.code(additional_tables_sql, language="sql")
            
            # Warnings and troubleshooting
            st.markdown("### Common Issues & Troubleshooting")
            st.warning("""
            **IMPORTANT NOTE:** If you encounter the error "Invalid object name", it means the HS tables were not created before running the load job.
            Make sure you follow the steps in order:
            1. Run SQL steps 1-4
            2. Run the Invalid HS Pipeline
            3. Create the HS table with SQL
            4. Complete the load with either option above
            """)
            
            # Store the complete SQL in session state for download
            st.session_state.all_sql = initial_setup_sql + "\n\n" + hs_table_sql + "\n\n" + cleanup_sql
            st.session_state.hs_quick_creation_sql = hs_quick_creation_sql
            st.session_state.st_placeholder_sql = st_placeholder_sql
            st.session_state.cleanup_sql = cleanup_sql
            
        except Exception as e:
            st.error(f"Error generating deployment steps: {str(e)}")
            st.error("Please check that the uploaded configuration contains all required parameters.")
            st.session_state.all_sql = "-- Error generating SQL scripts. Please check your configuration."
    
    with deploy_tab3:
        st.subheader("Download Files")
        
        # Create SQL files section
        st.markdown("### SQL Scripts")
        col1, col2 = st.columns(2)
        
        with col1:
            if 'all_sql' in st.session_state and st.session_state.all_sql:
                setup_sql_name = f"step1_4_initial_setup_{st.session_state.src_table_name.lower()}_{table_suffix}.sql"
                st.download_button(
                    label=f"1. Initial Setup SQL (Steps 1-4)",
                    data=initial_setup_sql,
                    file_name=setup_sql_name,
                    mime="text/plain",
                    key="download_setup_sql",
                    help="Download the SQL for initial setup (Steps 1-4)"
                )
            
            if 'hs_table_sql' in locals() and hs_table_sql:
                hs_sql_name = f"step6_hs_table_{st.session_state.src_table_name.lower()}_{table_suffix}.sql"
                st.download_button(
                    label=f"2. HS Table Creation SQL (Step 6)",
                    data=hs_table_sql,
                    file_name=hs_sql_name,
                    mime="text/plain",
                    key="download_hs_sql",
                    help="Download just the HS table creation script (Step 6)"
                )
        
        with col2:
            # Additional tables SQL if they exist
            if 'additional_tables_sql' in locals() and additional_tables_sql:
                add_tables_name = f"step8_additional_tables_{st.session_state.src_table_name.lower()}_{table_suffix}.sql"
                st.download_button(
                    label=f"3. Additional Tables SQL (Step 8)",
                    data=additional_tables_sql,
                    file_name=add_tables_name,
                    mime="text/plain",
                    key="download_add_tables_sql",
                    help="Download the SQL for additional tables (Step 8)"
                )
            
            # Cleanup SQL
            if 'cleanup_sql' in locals() and cleanup_sql:
                cleanup_sql_name = f"step10_cleanup_{st.session_state.src_table_name.lower()}_{table_suffix}.sql"
                st.download_button(
                    label=f"4. Cleanup SQL (Step 10)",
                    data=cleanup_sql,
                    file_name=cleanup_sql_name,
                    mime="text/plain",
                    key="download_cleanup_sql",
                    help="Download the SQL for cleanup (Step 10)"
                )
            
            # ST_Placeholder SQL
            if 'st_placeholder_sql' in locals() and st_placeholder_sql:
                placeholder_sql_name = f"step7_st_placeholder_{st.session_state.src_table_name.lower()}_{table_suffix}.sql"
                st.download_button(
                    label=f"ST_Placeholder SQL (Step 7 Option A)",
                    data=st_placeholder_sql,
                    file_name=placeholder_sql_name,
                    mime="text/plain",
                    key="download_placeholder_sql",
                    help="Download the SQL to update job control table for ST_Placeholder"
                )
            
            # Complete SQL as a single file
            if 'all_sql' in st.session_state and st.session_state.all_sql:
                complete_sql_name = f"complete_sql_{st.session_state.src_table_name.lower()}_{table_suffix}.sql"
                all_sql = initial_setup_sql + "\n\n" + hs_table_sql
                if 'cleanup_sql' in locals():
                    all_sql += "\n\n" + cleanup_sql
                if 'st_placeholder_sql' in locals():
                    all_sql += "\n\n" + st_placeholder_sql
                if 'additional_tables_sql' in locals() and additional_tables_sql:
                    all_sql += "\n\n" + additional_tables_sql
                
                st.download_button(
                    label=f"Complete SQL Script (All Steps)",
                    data=all_sql,
                    file_name=complete_sql_name,
                    mime="text/plain",
                    key="download_complete_sql",
                    help="Download all SQL scripts combined"
                )
        
        # ADF pipeline section
        st.markdown("### ADF Pipeline Files")
        st.markdown("""
        **Note:** These pipeline files are provided for reference only. 
        The pipelines should already exist in your environment after branch deployment.
        """)
        
        try:
            from src.utils.adf_generator import generate_adf_pipeline_json
            
            # Generate the ADF pipeline JSON for all pipeline types
            adf_json_invalid_hs = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                True, 
                True, 
                False,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            adf_json_initial = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                True, 
                False, 
                False,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            adf_json_daily = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                False, 
                False, 
                False,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            adf_json_placeholder = generate_adf_pipeline_json(
                st.session_state.src_table_name, 
                table_suffix, 
                True, 
                False, 
                True,
                st.session_state.source_system_initial,
                st.session_state.source_system_daily
            )
            
            # Convert the Python dictionaries to formatted JSON strings
            adf_json_str_invalid_hs = json.dumps(adf_json_invalid_hs, indent=4)
            adf_json_str_initial = json.dumps(adf_json_initial, indent=4)
            adf_json_str_daily = json.dumps(adf_json_daily, indent=4)
            adf_json_str_placeholder = json.dumps(adf_json_placeholder, indent=4)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.download_button(
                    label="1. Invalid HS Pipeline (Reference)",
                    data=adf_json_str_invalid_hs,
                    file_name=f"{adf_json_invalid_hs['name']}.json",
                    mime="application/json",
                    key="download_adf_invalid_hs",
                    help="Pipeline configuration for reference - should already exist in ADF"
                )
                
                st.download_button(
                    label="2. HS-only Pipeline (Reference)",
                    data=adf_json_str_placeholder,
                    file_name=f"{adf_json_placeholder['name']}.json",
                    mime="application/json",
                    key="download_adf_placeholder",
                    help="Pipeline configuration for reference - should already exist in ADF"
                )
            
            with col2:
                st.download_button(
                    label="3. Initial Load Pipeline (Reference)",
                    data=adf_json_str_initial,
                    file_name=f"{adf_json_initial['name']}.json",
                    mime="application/json",
                    key="download_adf_initial",
                    help="Pipeline configuration for reference - should already exist in ADF"
                )
                
                st.download_button(
                    label="4. Daily Load Pipeline (Reference)",
                    data=adf_json_str_daily,
                    file_name=f"{adf_json_daily['name']}.json",
                    mime="application/json",
                    key="download_adf_daily",
                    help="Pipeline configuration for reference - should already exist in ADF"
                )
                
        except Exception as e:
            st.error(f"Error generating ADF pipeline JSONs: {str(e)}")
            st.error("Please check that the uploaded configuration contains all required parameters.")

def render_deployer_view():
    """Main function to render the deployer view"""
    # Create a container for the sidebar
    with st.sidebar:
        render_deployer_sidebar()
    
    # Create a container for the main content
    with st.container():
        render_deployment_instructions() 