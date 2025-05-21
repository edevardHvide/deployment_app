# Data Warehouse Deployment App

A Streamlit application for generating SQL deployment scripts and ADF pipeline configurations for data warehouse tables.

## Overview

This application helps automate the process of deploying tables to a data warehouse by generating:
- SQL scripts for control table configurations
- SQL scripts for table creation
- Azure Data Factory (ADF) pipeline JSON configurations

## Project Structure

```
deployment_app/
├── app.py                 # Main application file
├── src/
│   ├── components/        # UI components
│   │   ├── sidebar.py     # Sidebar UI components
│   │   └── main_content.py # Main content UI components
│   ├── utils/            # Utility functions
│   │   ├── parameters.py  # Parameter handling functions
│   │   ├── sql_generator.py # SQL generation functions
│   │   └── adf_generator.py # ADF pipeline generation functions
│   └── config/           # Configuration
│       └── constants.py   # Constants and default values
└── README.md             # This file
```

## Features

- **Parameter Management**
  - Export current parameters to JSON
  - Import parameters from JSON file
  - Save and load configurations

- **Table Configuration**
  - Source table configuration
  - Target table configuration
  - Key columns configuration
  - Incremental load settings
  - SCD (Slowly Changing Dimension) configuration
  - Delete handling configuration

- **SQL Generation**
  - Control table backup scripts
  - ST (Stage) control table updates
  - HS (Historic Stage) control table updates
  - Job control table updates
  - HS table creation scripts
  - Dimension and helper table creation scripts

- **ADF Pipeline Generation**
  - Initial load pipeline configuration
  - Daily load pipeline configuration
  - Download pipeline JSON files

## Setup

1. Install the required dependencies:
```bash
pip install streamlit pandas
```

2. Run the application:
```bash
streamlit run app.py
```

## Usage

1. **Configure Parameters**
   - Enter your initials
   - Configure source and target table details
   - Set up key columns and incremental load settings
   - Configure SCD and delete handling options

2. **Generate Scripts**
   - Click "Generate SQL Script" to create all necessary scripts
   - Review the generated scripts in the tabs
   - Download individual scripts or the complete SQL script

3. **ADF Pipeline Setup**
   - Review the generated ADF pipeline JSON
   - Download the pipeline configurations
   - Follow the instructions for pasting into ADF

## Parameter Export/Import

- Use the "Export Parameters" button to save your current configuration
- Use the "Import Parameters" button to load a previously saved configuration
- Parameters are saved in JSON format

## Notes

- The application generates temporary control tables in the `sandbox` schema
- ADF pipelines use these temporary control tables for configuration
- After successful initial load, update the control tables to use daily load job names

## Contributing

Feel free to submit issues and enhancement requests! 