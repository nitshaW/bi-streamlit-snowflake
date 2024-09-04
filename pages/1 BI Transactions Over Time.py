import streamlit as st
import plotly.express as px
import pandas as pd
import sys
import os

# Ensure the data_store module can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
import data_store as ds

st.set_page_config(layout="wide")
st.title("Transaction Analysis")

# Add a "Clear Cache" button at the top
if st.button("Clear Cache"):
    ds.clear_cache()
    st.success("Cache cleared successfully!")

sel = "Transaction Value Analysis Over Time"
st.markdown(f"**{sel}**")
value_chart_tab, value_dataframe_tab = st.tabs(["Chart", "Tabular Data"])
summary_tab = st.sidebar.expander("Data Summary")

# SQL query to retrieve data
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

# Load data using the data_store function
df = ds.get_dataframe(query)

if df is not None:
    st.sidebar.header("Filters")
    
    # Load saved filters from data_store
    filters = ds.get_filters()
    
    # Date Range Filter
    filter_type = st.sidebar.radio("Select Date Filter Type", ("Transaction Date", "Event Date"))
    filters["date_column"] = "FB_CREATESERVICETSTAMP" if filter_type == "Transaction Date" else "FB_SERVICE_DATE"
    
    date_filter = st.sidebar.date_input("Select Date Range", filters.get("date_range", []))
    if len(date_filter) == 2:
        filters["date_range"] = date_filter

    # Apply initial filters to the DataFrame for cascading effect
    df_filtered = ds.filter_data(df, {"date_range": filters.get("date_range"), "date_column": filters["date_column"]})

    # Cascading Filters
    corporate_filter = st.sidebar.multiselect("Select Corporate Entity", df_filtered["VN_CORPORATE_ENTITY_NAME"].unique(), default=filters.get("corporate_entity", []))
    if corporate_filter:
        filters["corporate_entity"] = corporate_filter
        df_filtered = df_filtered[df_filtered["VN_CORPORATE_ENTITY_NAME"].isin(corporate_filter)]
    
    management_filter = st.sidebar.multiselect("Select Management Entity", df_filtered["VN_MANAGEMENT_ENTITY_NAME"].unique(), default=filters.get("management_entity", []))
    if management_filter:
        filters["management_entity"] = management_filter
        df_filtered = df_filtered[df_filtered["VN_MANAGEMENT_ENTITY_NAME"].isin(management_filter)]
    
    # Separate Dropdowns for Venue Type, Global Type, and Pay Type
    venue_type_filter = st.sidebar.multiselect("Select Venue Type", df_filtered["VN_VENUE_TYPE_NAME"].unique(), default=filters.get("venue_type", []))
    if venue_type_filter:
        filters["venue_type"] = venue_type_filter
        df_filtered = df_filtered[df_filtered["VN_VENUE_TYPE_NAME"].isin(venue_type_filter)]

    global_type_filter = st.sidebar.multiselect("Select Global Type", df_filtered["FB_GLOBALTYPE_DESC"].unique(), default=filters.get("global_type", []))
    if global_type_filter:
        filters["global_type"] = global_type_filter
        df_filtered = df_filtered[df_filtered["FB_GLOBALTYPE_DESC"].isin(global_type_filter)]

    pay_type_filter = st.sidebar.multiselect("Select Pay Type", df_filtered["FB_PAYTYPE_DESC"].unique(), default=filters.get("pay_type", []))
    if pay_type_filter:
        filters["pay_type"] = pay_type_filter
        df_filtered = df_filtered[df_filtered["FB_PAYTYPE_DESC"].isin(pay_type_filter)]
    
    venue_filter = st.sidebar.multiselect("Select Venue", df_filtered["VN_VENUE_NAME"].unique(), default=filters.get("venue", []))
    if venue_filter:
        filters["venue"] = venue_filter
        df_filtered = df_filtered[df_filtered["VN_VENUE_NAME"].isin(venue_filter)]
    
    pay_status_filter = st.sidebar.multiselect("Select Pay Status", df_filtered["FB_PAYACTION_DESC"].unique(), default=filters.get("pay_status", []))
    if pay_status_filter:
        filters["pay_status"] = pay_status_filter
        df_filtered = df_filtered[df_filtered["FB_PAYACTION_DESC"].isin(pay_status_filter)]
    
    # Save updated filters back to data_store
    ds.save_filters(filters)

    # Final data check and visualization
    if df_filtered.empty:
        st.error("No data available with the current filters. Please select different filters.")
    else:
        # Ensure the dates are properly aggregated by day and month
        df_filtered['Date'] = df_filtered['FB_CREATESERVICETSTAMP'].dt.date
        df_filtered['YearMonth'] = df_filtered['FB_CREATESERVICETSTAMP'].dt.to_period('M').astype(str)
        
        # Group by Date and Month
        df_grouped_day = df_filtered.groupby('Date').agg({
            'FB_CHARGE_AMOUNT': 'sum',
            'FB_SPENDAGREE_AMOUNT': 'sum',
            'FB_SUBTOTAL_AMOUNT': 'sum',
            'FB_PLANNED_GUEST_COUNT': 'sum'
        }).reset_index()
        
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