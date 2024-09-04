import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import configparser
import os
from snowflake.snowpark.context import get_active_session

@st.cache_resource
def get_session():
    """
    Establishes and returns a Snowflake session using credentials from the SnowSQL config file.
    Caches the session to avoid reloading on every execution.
    """
    try:
        return get_active_session()
    except:
        parser = configparser.ConfigParser()
        parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
        section = "connections.demo_conn"
        pars = {
            "account": parser.get(section, "account"),
            "user": parser.get(section, "username"),
            "password": parser.get(section, "password"),
            "warehouse": parser.get(section, "warehousename"),
            "role": parser.get(section, "role"),
            "client_session_keep_alive": True
        }
        return Session.builder.configs(pars).create()

@st.cache_data(show_spinner=False)
def get_dataframe(query):
    """
    Executes a SQL query on the Snowflake session and returns the results as a pandas DataFrame.
    Caches the DataFrame to avoid reloading on every execution.
    """
    session = get_session()
    if session is None:
        st.error("Session is not initialized.")
        return None
    try:
        snow_df = session.sql(query).to_pandas()
        snow_df = snow_df.drop_duplicates()

        # Convert relevant columns to datetime with error handling
        snow_df['FB_CREATESERVICETSTAMP'] = pd.to_datetime(snow_df['FB_CREATESERVICETSTAMP'], unit='s', errors='coerce')
        snow_df['FB_SERVICE_DATE'] = pd.to_datetime(snow_df['FB_SERVICE_DATE'], format='%m/%d/%Y', errors='coerce')
        
        # Handle missing or erroneous dates by dropping or setting a default date
        snow_df.dropna(subset=['FB_CREATESERVICETSTAMP', 'FB_SERVICE_DATE'], inplace=True)
        
        return snow_df
    except Exception as e:
        st.error(f"Failed to execute query or process data: {str(e)}")
        return None

@st.cache_data(show_spinner=False)
def filter_data(df, filters):
    """
    Apply filters to the dataframe based on the given filter criteria.
    :param df: Original dataframe
    :param filters: A dictionary of filters to apply
    :return: Filtered dataframe
    """
    df_filtered = df.copy()

    if filters.get("date_range"):
        date_col = filters["date_column"]
        start_date, end_date = filters["date_range"]
        df_filtered = df_filtered[(df_filtered[date_col] >= pd.Timestamp(start_date)) & 
                                  (df_filtered[date_col] <= pd.Timestamp(end_date))]

    if filters.get("corporate_entity"):
        df_filtered = df_filtered[df_filtered["VN_CORPORATE_ENTITY_NAME"].isin(filters["corporate_entity"])]
    
    if filters.get("management_entity"):
        df_filtered = df_filtered[df_filtered["VN_MANAGEMENT_ENTITY_NAME"].isin(filters["management_entity"])]
    
    if filters.get("venue"):
        df_filtered = df_filtered[df_filtered["VN_VENUE_NAME"].isin(filters["venue"])]
    
    if filters.get("venue_type"):
        df_filtered = df_filtered[df_filtered["VN_VENUE_TYPE_NAME"].isin(filters["venue_type"])]
    
    if filters.get("global_type"):
        df_filtered = df_filtered[df_filtered["FB_GLOBALTYPE_DESC"].isin(filters["global_type"])]
    
    if filters.get("pay_type"):
        df_filtered = df_filtered[df_filtered["FB_PAYTYPE_DESC"].isin(filters["pay_type"])]
    
    if filters.get("pay_status"):
        df_filtered = df_filtered[df_filtered["FB_PAYACTION_DESC"].isin(filters["pay_status"])]
    
    return df_filtered

def clear_cache():
    """
    Clear cached data and resources.
    """
    st.cache_data.clear()
    st.cache_resource.clear()

def get_filters():
    """
    Retrieve stored filters from session state.
    If no filters are stored, return an empty dictionary.
    """
    return st.session_state.get('filters', {})

def save_filters(filters):
    """
    Save the given filters to session state.
    :param filters: A dictionary of filters to save
    """
    st.session_state['filters'] = filters