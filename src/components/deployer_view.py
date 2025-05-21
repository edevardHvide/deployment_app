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
    
    # Get the table suffix
    table_suffix = st.session_state.table_suffix
    
    # Display table information
    st.markdown(f"""
    ## Deployment for: {st.session_state.src_table_name}
    
    **Configuration uploaded by:** {st.session_state.get('user_initials', 'Developer')}  
    **Target tables:**
    - ST table: `{st.session_state.tgt_schema_name_st}.{st.session_state.tgt_table_name_st}`
    - HS table: `{st.session_state.tgt_schema_name_hs}.{st.session_state.tgt_table_name_hs}`
    """)
    
    # Create tabs for deployment steps
    deploy_tab1, deploy_tab2, deploy_tab3 = st.tabs([
        "1. SQL Scripts", 
        "2. Deployment Instructions",
        "3. Download Files"
    ])
    
    with deploy_tab1:
        st.subheader("SQL Deployment Scripts")
        
        try:
            # Generate all SQL scripts
            backup_sql = generate_control_table_backup_sql(table_suffix)
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
                st.session_state.get("prescript", ""),
                st.session_state.get("postscript", ""),
                st.session_state.get("partitions", 1),
                st.session_state.get("use_source_column_for_valid_dates", True),
                st.session_state.get("source_column_for_valid_from_date", ""),
                st.session_state.get("source_column_for_sorting", ""),
                st.session_state.source_system_initial
            )
            job_control_sql = generate_job_control_sql(table_suffix)
            hs_table_sql = generate_hs_table_sql(
                st.session_state.tgt_schema_name_hs,
                st.session_state.tgt_table_name_hs,
                st.session_state.tgt_schema_name_st,
                st.session_state.tgt_table_name_st
            )
            
            # Helper & Main Table SQL (if configured)
            helper_table_sql = generate_helper_table_sql(
                st.session_state.get("create_helper_table", False),
                st.session_state.get("helper_schema", ""),
                st.session_state.get("business_key_column", ""),
                st.session_state.src_table_name
            )
            
            main_table_sql = generate_main_table_sql(
                st.session_state.get("create_main_table", False),
                st.session_state.get("main_table_schema", ""),
                st.session_state.get("main_table_columns", ""),
                st.session_state.src_table_name,
                st.session_state.get("main_table_name", "")
            )
            
            # Combine all SQL into one complete script
            complete_sql = f"""-- Generated SQL Deployment Script for {st.session_state.src_table_name}
-- Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- Table suffix: {table_suffix}

---------------------------------------------------------
-- STEP 1: CREATE TEMPORARY CONTROL TABLES
---------------------------------------------------------
{backup_sql}

---------------------------------------------------------
-- STEP 2: UPDATE ST CONTROL TABLE
---------------------------------------------------------
{st_control_sql}

---------------------------------------------------------
-- STEP 3: UPDATE HS CONTROL TABLE
---------------------------------------------------------
{hs_control_sql}

---------------------------------------------------------
-- STEP 4: UPDATE JOB CONTROL TABLE
---------------------------------------------------------
{job_control_sql}

---------------------------------------------------------
-- STEP 5: CREATE HS TABLE
---------------------------------------------------------
{hs_table_sql}
"""
            
            # Add helper and main table SQL if they exist
            if helper_table_sql:
                complete_sql += f"""
---------------------------------------------------------
-- STEP 6: CREATE HELPER TABLE
---------------------------------------------------------
{helper_table_sql}
"""
            
            if main_table_sql:
                complete_sql += f"""
---------------------------------------------------------
-- STEP 7: CREATE MAIN TABLE
---------------------------------------------------------
{main_table_sql}
"""
            
            # Display the complete SQL
            st.code(complete_sql, language="sql")
            
            # Store the complete SQL in session state for download
            st.session_state.all_sql = complete_sql
            
        except Exception as e:
            st.error(f"Error generating SQL scripts: {str(e)}")
            st.error("Please check that the uploaded configuration contains all required parameters.")
            st.session_state.all_sql = "-- Error generating SQL scripts. Please check your configuration."
    
    with deploy_tab2:
        st.subheader("Step-by-Step Deployment Instructions")
        
        st.markdown("""
        ### 1. Run the SQL Script
        
        - Connect to your SQL Server database
        - Run the complete SQL script from the "SQL Scripts" tab
        - This will:
          - Create temporary control tables
          - Configure the ST and HS control tables
          - Set up the job control settings
          - Create the HS table structure
          - Create helper and dimension tables (if configured)
        
        ### 2. Deploy the ADF Pipelines
        
        - Locate the Pull Request (PR) created by the developer in your Azure DevOps or GitHub environment
        - Review the PR changes to ensure they contain the required pipeline configurations
        - Approve and merge the PR into the relevant environment
        - Verify that the pipelines appear in your Azure Data Factory workspace after the merge
        
        ### 3. Run the Initial Load
        
        - In ADF, manually trigger the initial load pipeline
        - This will populate the ST table and create the HS table
        
        ### 4. Verify the Deployment
        
        - Check that data is flowing correctly into the ST and HS tables
        - Verify the HS table structure includes all technical columns
        - If helper or dimension tables were configured, check they were created properly
        
        ### 5. Schedule the Daily Load
        
        - Set up the ADF trigger for the daily load pipeline
        - Recommended: Set up monitoring to verify successful daily runs
        """)
    
    with deploy_tab3:
        st.subheader("Download Files")
        
        # Create download buttons for the SQL script
        st.download_button(
            label="Download Complete SQL Script",
            data=st.session_state.all_sql,
            file_name=f"deploy_{st.session_state.src_table_name}_{table_suffix}.sql",
            mime="text/plain",
            use_container_width=True
        )
        
        # Generate and provide download for ADF JSON files
        try:
            from src.utils.adf_generator import generate_adf_pipeline_json
            
            # Generate the ADF pipeline JSON for both initial and daily loads
            adf_json_initial = generate_adf_pipeline_json(st.session_state.src_table_name, table_suffix, True)
            adf_json_daily = generate_adf_pipeline_json(st.session_state.src_table_name, table_suffix, False)
            
            # Convert the Python dictionaries to formatted JSON strings
            adf_json_str_initial = json.dumps(adf_json_initial, indent=4)
            adf_json_str_daily = json.dumps(adf_json_daily, indent=4)
            
            st.download_button(
                label="Download Initial Load ADF Pipeline JSON",
                data=adf_json_str_initial,
                file_name=f"{adf_json_initial['name']}.json",
                mime="application/json",
                use_container_width=True
            )
            
            st.download_button(
                label="Download Daily Load ADF Pipeline JSON",
                data=adf_json_str_daily,
                file_name=f"{adf_json_daily['name']}.json",
                mime="application/json",
                use_container_width=True
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