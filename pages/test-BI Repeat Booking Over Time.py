import streamlit as st
from snowflake.snowpark import Session
import plotly.express as px
import pandas as pd
import os, configparser
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")
st.title("Repeat Booking Analysis")

# Add a "Clear Cache" button at the top
if st.button("Clear Cache"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.success("Cache cleared successfully!")

sel = "Repeat Booking Analysis Over Time"
st.markdown(f"**{sel}**")
value_chart_tab, value_dataframe_tab = st.tabs(["Chart", "Tabular Data"])
summary_tab = st.sidebar.expander("Data Summary")

@st.cache_resource
def get_session():
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

@st.cache_data
def get_dataframe(query):
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

query = """
    SELECT 
    fb.BOOK_TRANS_WID as FB_BOOK_TRANS_WID,
    fb.BOOK_TRANS_ID as FB_BOOK_TRANS_ID,
    fb.VISIT_ID as FB_VISIT_ID,
    fb.CORPORATE_ENTITY_ID as FB_CORPORATE_ENTITY_ID,
    fb.MANAGEMENT_ENTITY_ID as FB_MANAGEMENT_ENTITY_ID,
    fb.VENUE_ID as FB_VENUE_ID,
    fb.SOURCE_SYSTEMS as FB_SOURCE_SYSTEMS,
    fb.SERVICE_ID as FB_SERVICE_ID,
    fb.CREATESERVICETSTAMP as FB_CREATESERVICETSTAMP,
    fb.MODSERVICETSTAMP as FB_MODSERVICETSTAMP,
    fb.SERVICE_DATE as FB_SERVICE_DATE,
    fb.TRANSTIXREF as FB_TRANSTIXREF,
    fb.BILLED_NAME as FB_BILLED_NAME,
    fb.CART_ID as FB_CART_ID,
    fb.CHARGE_AMOUNT as FB_CHARGE_AMOUNT,
    fb.CITY as FB_CITY,
    fb.COUNTRY_CODE as FB_COUNTRY_CODE,
    fb.EMAIL as FB_EMAIL,
    fb.EVENT_ID as FB_EVENT_ID,
    fb.GLOBALTYPE_DESC as FB_GLOBALTYPE_DESC,
    fb.ITEM_NAME as FB_ITEM_NAME,
    fb.MASTERITEM_ID as FB_MASTERITEM_ID,
    fb.PARTY_ID as FB_PARTY_ID, 
    fb.PAYACTION_DESC as FB_PAYACTION_DESC,
    fb.PAYTYPE_DESC as FB_PAYTYPE_DESC,
    fb.PLANNED_GUEST_COUNT as FB_PLANNED_GUEST_COUNT,
    fb.PRESALE_TRANS_ID as FB_PRESALE_TRANS_ID,
    fb.PROVINCE_CODE as FB_PROVINCE_CODE,
    fb.SPENDAGREE_AMOUNT as FB_SPENDAGREE_AMOUNT,
    fb.SUBTOTAL_AMOUNT as FB_SUBTOTAL_AMOUNT,
    fb.TIXID as FB_TIXID,
    fb.TRANSTIXID as FB_TRANSTIXID,
    fb.ZIP as FB_ZIP,
    
    vs.VISIT_WID as VS_VISIT_WID,
    vs.CURRENTSTATE_DESC as VS_CURRENTSTATE_DESC,
    vs.COMPAGREE_AMOUNT as VS_COMPAGREE_AMOUNT,
    vs.ORIGINATOR_ID as VS_ORIGINATOR_ID,
    vs.OWNER_ID as VS_OWNER_ID,
    vs.SPENDAGREE_AMOUNT as VS_SPENDAGREE_AMOUNT,
    vs.SOURCE_CODE as VS_SOURCE_CODE,
    vs.CANCELSTATE_DESC as VS_CANCELSTATE_DESC,
    vs.SOURCE_LOC as VS_SOURCE_LOC,

    vn.VENUE_RECORD_STATUS as VN_VENUE_RECORD_STATUS,
    vn.CORPORATE_ENTITY_NAME as VN_CORPORATE_ENTITY_NAME,
    vn.MANAGEMENT_ENTITY_NAME as VN_MANAGEMENT_ENTITY_NAME,
    vn.VENUE_NAME as VN_VENUE_NAME,
    vn.VENUE_MARKET_AREA_NAME as VN_VENUE_MARKET_AREA_NAME,
    vn.VENUE_TYPE_NAME as VN_VENUE_TYPE_NAME,
    vn.VENUE_CITY as VN_VENUE_CITY,
    vn.VENUE_PROVINCE as VN_VENUE_PROVINCE,
    vn.VENUE_COUNTRY as VN_VENUE_COUNTRY,

    it.ITEM_ID as IT_ITEM_ID,
    it.ITEM_GLOBALTYPE_CODE as IT_ITEM_GLOBALTYPE_CODE,
    it.ITEM_PREFAB as IT_ITEM_PREFAB,
    it.ITEM_PRICINGS as IT_ITEM_PRICINGS,
    it.ITEM_PUBLICNAME as IT_ITEM_PUBLICNAME,
    it.ITEM_BOOKTYPE_NAME as IT_ITEM_BOOKTYPE_NAME,
    it.ITEM_TYPE_CODE_NAME as IT_ITEM_TYPE_CODE_NAME

    FROM edw.public.fact_book_trans fb
    LEFT JOIN edw.public.dim_visit vs on fb.visit_id = vs.visit_id
    LEFT JOIN edw.public.dim_venue vn on vs.venue_id = vn.venue_id
    LEFT JOIN edw.public.dim_item it on fb.masteritem_id = it.item_id
    WHERE fb.source_systems IN ('PAY', 'urcheckout')
"""

df = get_dataframe(query)

if df is not None:
    st.sidebar.header("Filters")
    
    # Date Range Filter
    filter_type = "Event Date"  # Automatically set the filter to "Event Date"
    df_filtered = df.copy()

    date_filter = st.sidebar.date_input("Select Event Date Range", [])
    if len(date_filter) == 2:
        df_filtered = df_filtered[(df_filtered['FB_SERVICE_DATE'] >= pd.Timestamp(date_filter[0])) & 
                                   (df_filtered['FB_SERVICE_DATE'] <= pd.Timestamp(date_filter[1]))]

    # Ensure at least one non-date filter is selected
    filters_selected = False
    
    # Cascading Filters
    corporate_filter = st.sidebar.multiselect("Select Corporate Entity", df_filtered["VN_CORPORATE_ENTITY_NAME"].unique(), default=[])
    if corporate_filter:
        df_filtered = df_filtered[df_filtered["VN_CORPORATE_ENTITY_NAME"].isin(corporate_filter)]
        filters_selected = True
    
    management_filter = st.sidebar.multiselect("Select Management Entity", df_filtered["VN_MANAGEMENT_ENTITY_NAME"].unique(), default=[])
    if management_filter:
        df_filtered = df_filtered[df_filtered["VN_MANAGEMENT_ENTITY_NAME"].isin(management_filter)]
        filters_selected = True
    
    venue_filter = st.sidebar.multiselect("Select Venue", df_filtered["VN_VENUE_NAME"].unique(), default=[])
    if venue_filter:
        df_filtered = df_filtered[df_filtered["VN_VENUE_NAME"].isin(venue_filter)]
        filters_selected = True
    
    # Either Venue Type or Global Type Filter
    filter_choice = st.sidebar.radio("Filter by:", ["Venue Type", "Global Type"])

    if filter_choice == "Venue Type":
        venue_type_filter = st.sidebar.multiselect("Select Venue Type", df_filtered["VN_VENUE_TYPE_NAME"].unique(), default=[])
        if venue_type_filter:
            df_filtered = df_filtered[df_filtered["VN_VENUE_TYPE_NAME"].isin(venue_type_filter)]
            filters_selected = True
    elif filter_choice == "Global Type":
        global_type_filter = st.sidebar.multiselect("Select Global Type", df_filtered["FB_GLOBALTYPE_DESC"].unique(), default=[])
        if global_type_filter:
            df_filtered = df_filtered[df_filtered["FB_GLOBALTYPE_DESC"].isin(global_type_filter)]
            filters_selected = True

    if df_filtered.empty:
        st.error("No data available with the current filters. Please select different filters.")
    else:
        # Ensure the dates are properly aggregated by month
        df_filtered['YearMonth'] = df_filtered['FB_CREATESERVICETSTAMP'].dt.to_period('M').astype(str)
        
        # Identify repeat bookings by counting occurrences of FB_VISIT_ID for each email
        df_repeat = df_filtered.groupby('FB_EMAIL').filter(lambda x: len(x['FB_VISIT_ID'].unique()) > 1)
        
        # Count the number of repeat bookings over time (monthly)
        df_grouped_month = df_repeat.groupby('YearMonth').agg({
            'FB_VISIT_ID': pd.Series.nunique
        }).reset_index().rename(columns={'FB_VISIT_ID': 'Repeat_Bookings'})

        # Initialize the chart for monthly repeat bookings
        fig = px.line(df_grouped_month, x='YearMonth', y='Repeat_Bookings', 
                      title="Monthly Repeat Bookings Over Time", labels={'YearMonth': 'Month', 'Repeat_Bookings': 'Number of Repeat Bookings'})

        # Display chart in the tab
        with value_chart_tab:
            st.plotly_chart(fig, use_container_width=True)
        
        # Display data frame in the tab
        with value_dataframe_tab:
            st.write("Repeat Booking Data - Monthly View")
            st.dataframe(df_grouped_month, height=400, width=1000)
else:
    st.error("Failed to retrieve data.")