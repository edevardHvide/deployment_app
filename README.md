# Data Warehouse Table Deployment Helper

A Streamlit application that automates the process of deploying tables in a data warehouse by generating the necessary SQL scripts.

## Features

- Generates SQL code for all deployment steps
- Organizes the deployment process into logical tabs
- Customizable table properties and configuration
- Options to skip steps for tables that already exist
- User identification with initials in table names
- Dated timestamps in table names for tracking
- Download complete SQL script as a single executable file

## Installation

1. Clone this repository
2. Install the requirements:
```
pip install -r requirements.txt
```

## Usage

1. Run the Streamlit app:
```
streamlit run app.py
```

2. Fill in the required table information in the sidebar:
   - Your initials for table name identification
   - Source table information
   - Business key
   - SCD2 columns for HS table
   - Delete configuration

3. Click the "Generate SQL Script" button in the sidebar to create deployment scripts

4. Navigate through the tabs to see SQL for each step:
   - Control Tables Backup
   - ST Control Table configuration
   - HS Control Table configuration
   - Job Control Table updates
   - HS Table creation
   - Cleanup steps

5. Use the "Skip Options" checkboxes in the sidebar if certain tables already exist

6. Download the complete SQL script using the download button at the bottom of the page

## Deployment Process

The app follows a standardized deployment process:

1. Creates temporary tables from existing control tables
2. Updates configuration for stage tables (ST)
3. Updates configuration for historic stage tables (HS)
4. Updates job control settings
5. Creates the HS table with technical columns
6. Adds configurations to production control tables and performs cleanup

## Download Feature

The app provides a convenient way to download the complete SQL script as a single file:
- All steps are combined into one ready-to-execute SQL file
- The file is named with the table name, user initials, and timestamp (e.g., `deploy_TABLENAME_initials_20240515.sql`)
- Comments and section headers are added for readability
- Skip options are respected in the downloaded file

## Table Naming Convention

The app uses a specific naming convention for tables:
- Temporary tables are named with the pattern: `temp_control_table_st_initials_YYYYMMDD`
- This ensures tables are identifiable by both creator and creation date
- Example: `temp_control_table_st_skg_20240515` for tables created by user "skg" on May 15, 2024

## Notes

- All dangerous cleanup commands (DROP, DELETE) are commented out for safety
- All SQL is displayed for review before execution
- The app doesn't execute SQL directly; it generates scripts for review and manual execution 