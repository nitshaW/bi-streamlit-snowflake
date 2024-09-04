import streamlit as st
from snowflake.snowpark import Session
import plotly.express as px
import pandas as pd
import os, configparser
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")
st.title("Transaction Analysis")

# Add a "Clear Cache" button at the top
if st.button("Clear Cache"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.success("Cache cleared successfully!")

sel = "Transaction Value Analysis Over Time"
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
    filter_type = st.sidebar.radio("Select Date Filter Type", ("Transaction Date", "Event Date"))
    df_filtered = df.copy()
    if filter_type == "Transaction Date":
        date_filter = st.sidebar.date_input("Select Transaction Date Range", [])
        if len(date_filter) == 2:
            df_filtered = df_filtered[(df_filtered['FB_CREATESERVICETSTAMP'] >= pd.Timestamp(date_filter[0])) & 
                                       (df_filtered['FB_CREATESERVICETSTAMP'] <= pd.Timestamp(date_filter[1]))]
    else:
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
    
    type_filter_option = st.sidebar.radio("Select Filter Type", ["Venue Type", "Global Type", "Pay Type"], index=0)
    if type_filter_option == "Venue Type":
        type_filter = st.sidebar.multiselect("Select Venue Type", df_filtered["VN_VENUE_TYPE_NAME"].unique(), default=[])
        if type_filter:
            df_filtered = df_filtered[df_filtered["VN_VENUE_TYPE_NAME"].isin(type_filter)]
            filters_selected = True
    elif type_filter_option == "Global Type":
        type_filter = st.sidebar.multiselect("Select Global Type", df_filtered["FB_GLOBALTYPE_DESC"].unique(), default=[])
        if type_filter:
            df_filtered = df_filtered[df_filtered["FB_GLOBALTYPE_DESC"].isin(type_filter)]
            filters_selected = True
    elif type_filter_option == "Pay Type":
        type_filter = st.sidebar.multiselect("Select Pay Type", df_filtered["FB_PAYTYPE_DESC"].unique(), default=[])
        if type_filter:
            df_filtered = df_filtered[df_filtered["FB_PAYTYPE_DESC"].isin(type_filter)]
            filters_selected = True
    
    venue_filter = st.sidebar.multiselect("Select Venue", df_filtered["VN_VENUE_NAME"].unique(), default=[])
    if venue_filter:
        df_filtered = df_filtered[df_filtered["VN_VENUE_NAME"].isin(venue_filter)]
        filters_selected = True
    
    pay_status_filter = st.sidebar.multiselect("Select Pay Status", df_filtered["FB_PAYACTION_DESC"].unique(), default=[])
    if pay_status_filter:
        df_filtered = df_filtered[df_filtered["FB_PAYACTION_DESC"].isin(pay_status_filter)]
        filters_selected = True
    
    # If no other filters are selected, the filtered data should still respect the date range
    if df_filtered.empty:
        st.error("No data available with the current filters. Please select different filters.")
    else:
        # Ensure the dates are properly aggregated by day
        df_filtered['Date'] = df_filtered['FB_CREATESERVICETSTAMP'].dt.date
        df_filtered['YearMonth'] = df_filtered['FB_CREATESERVICETSTAMP'].dt.to_period('M').astype(str)
        
        # Group by Date
        df_grouped_day = df_filtered.groupby('Date').agg({
            'FB_CHARGE_AMOUNT': 'sum',
            'FB_SPENDAGREE_AMOUNT': 'sum',
            'FB_SUBTOTAL_AMOUNT': 'sum',
            'FB_PLANNED_GUEST_COUNT': 'sum'
        }).reset_index()
        
        # Group by YearMonth
        df_grouped_month = df_filtered.groupby('YearMonth').agg({
            'FB_CHARGE_AMOUNT': 'sum',
            'FB_SPENDAGREE_AMOUNT': 'sum',
            'FB_SUBTOTAL_AMOUNT': 'sum',
            'FB_PLANNED_GUEST_COUNT': 'sum'
        }).reset_index()

        # User selection for daily, monthly, or both views
        view_type = st.sidebar.radio("Select View Type", ["Daily", "Monthly", "Both"], index=2)
        
        # Initialize the charts
        fig1 = px.line(title="CHARGE_AMOUNT Over Time")
        fig2 = px.line(title="SPENDAGREE_AMOUNT Over Time")
        fig3 = px.line(title="SUBTOTAL_AMOUNT Over Time")
        fig4 = px.line(title="PLANNED_GUEST_COUNT Over Time")

        if view_type in ["Daily", "Both"]:
            fig1.add_scatter(x=df_grouped_day['Date'], y=df_grouped_day['FB_CHARGE_AMOUNT'], mode='lines', name='Daily CHARGE_AMOUNT')
            fig2.add_scatter(x=df_grouped_day['Date'], y=df_grouped_day['FB_SPENDAGREE_AMOUNT'], mode='lines', name='Daily SPENDAGREE_AMOUNT')
            fig3.add_scatter(x=df_grouped_day['Date'], y=df_grouped_day['FB_SUBTOTAL_AMOUNT'], mode='lines', name='Daily SUBTOTAL_AMOUNT')
            fig4.add_scatter(x=df_grouped_day['Date'], y=df_grouped_day['FB_PLANNED_GUEST_COUNT'], mode='lines', name='Daily PLANNED_GUEST_COUNT')
            
        if view_type in ["Monthly", "Both"]:
            fig1.add_scatter(x=df_grouped_month['YearMonth'].astype(str), y=df_grouped_month['FB_CHARGE_AMOUNT'], mode='lines', name='Monthly CHARGE_AMOUNT')
            fig2.add_scatter(x=df_grouped_month['YearMonth'].astype(str), y=df_grouped_month['FB_SPENDAGREE_AMOUNT'], mode='lines', name='Monthly SPENDAGREE_AMOUNT')
            fig3.add_scatter(x=df_grouped_month['YearMonth'].astype(str), y=df_grouped_month['FB_SUBTOTAL_AMOUNT'], mode='lines', name='Monthly SUBTOTAL_AMOUNT')
            fig4.add_scatter(x=df_grouped_month['YearMonth'].astype(str), y=df_grouped_month['FB_PLANNED_GUEST_COUNT'], mode='lines', name='Monthly PLANNED_GUEST_COUNT')

        # Display charts in the tab
        with value_chart_tab:
            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.plotly_chart(fig3, use_container_width=True)
            st.plotly_chart(fig4, use_container_width=True)
        
        # Display data frame in the tab based on selected view type
        with value_dataframe_tab:
            if view_type == "Daily":
                st.write("Transaction Value Data - Daily View")
                st.dataframe(df_grouped_day, height=400, width=1000)
            elif view_type == "Monthly":
                st.write("Transaction Value Data - Monthly View")
                st.dataframe(df_grouped_month, height=400, width=1000)
            elif view_type == "Both":
                st.write("Transaction Value Data - Daily and Monthly View")
                st.write("Daily Data")
                st.dataframe(df_grouped_day, height=200, width=1000)
                st.write("Monthly Data")
                st.dataframe(df_grouped_month, height=200, width=1000)
else:
    st.error("Failed to retrieve data.") 