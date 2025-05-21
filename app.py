import streamlit as st
from datetime import datetime
import json
from src.components.sidebar import render_sidebar
from src.components.main_content import render_main_content
from src.utils.parameters import export_parameters, import_parameters, get_current_params
from src.config.constants import DEFAULT_VALUES

# Set page config
st.set_page_config(
    page_title="Skuld DWH Deployment App",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/yourusername/deployment_app/issues',
        'Report a bug': 'https://github.com/yourusername/deployment_app/issues',
        'About': '''
        # Data Warehouse Deployment App
        This app helps automate the process of deploying tables to a data warehouse by generating SQL scripts and ADF pipeline configurations.
        '''
    }
)

# Initialize session state variables if they don't exist
if 'sql_generated' not in st.session_state:
    st.session_state.sql_generated = False

if 'table_suffix' not in st.session_state:
    st.session_state.table_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

if 'all_sql' not in st.session_state:
    st.session_state.all_sql = ""

if 'timestamp' not in st.session_state:
    st.session_state.timestamp = ""

# Initialize default values
for key, value in DEFAULT_VALUES.items():
    if key not in st.session_state:
        st.session_state[key] = value

# Create a container for the sidebar
with st.sidebar:
    st.title("Data Warehouse Deployment App")
    render_sidebar()

# Create a container for the main content
with st.container():
    render_main_content() 