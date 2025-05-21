import streamlit as st

def render_role_selector():
    """Render the role selection screen"""
    st.title("Data Warehouse Deployment App")
    
    st.markdown("""
    ## Select Your Role
    """)
    
    # Create a container with some padding
    with st.container():
        # Create two columns for the buttons
        left_col, right_col = st.columns(2)
        
        with left_col:
            st.markdown("### Developer")
            developer_button = st.button("Enter as Developer", 
                                        use_container_width=True, 
                                        type="primary")
        
        with right_col:
            st.markdown("### Deployer")
            deployer_button = st.button("Enter as Deployer", 
                                       use_container_width=True,
                                       type="primary")
    
    # Create two columns for the descriptions
    desc_left, desc_right = st.columns(2)
    
    with desc_left:
        st.info("""
        **Developer Role**
        
        Configure all table parameters, generate SQL scripts and ADF pipeline configurations, and export them for deployment.
        """)
    
    with desc_right:
        st.info("""
        **Deployer Role**
        
        Import configuration from developers, view simplified deployment instructions, and download required SQL scripts.
        """)
    
    # Set the user role in session state based on button clicks
    if developer_button:
        st.session_state.user_role = "developer"
        st.rerun()
    
    if deployer_button:
        st.session_state.user_role = "deployer"
        st.rerun() 