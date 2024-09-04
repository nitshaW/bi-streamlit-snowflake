import streamlit as st
import plotly.express as px
import pandas as pd
import sys
import os

# Ensure the data_store module can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
import data_store as ds

st.set_page_config(layout="wide")
st.title("Day of the Week Transaction Trend")

# Add a "Clear Cache" button at the top
if st.button("Clear Cache"):
    ds.clear_cache()
    st.success("Cache cleared successfully!")

sel = "Day of the Week Transaction Analysis"
st.markdown(f"**{sel}**")
trend_chart_tab, trend_dataframe_tab = st.tabs(["Chart", "Tabular Data"])
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
    filters["corporate_entity"] = st.sidebar.multiselect("Select Corporate Entity", df_filtered["VN_CORPORATE_ENTITY_NAME"].unique(), default=filters.get("corporate_entity", []))
    if filters["corporate_entity"]:
        df_filtered = df_filtered[df_filtered["VN_CORPORATE_ENTITY_NAME"].isin(filters["corporate_entity"])]
    
    filters["management_entity"] = st.sidebar.multiselect("Select Management Entity", df_filtered["VN_MANAGEMENT_ENTITY_NAME"].unique(), default=filters.get("management_entity", []))
    if filters["management_entity"]:
        df_filtered = df_filtered[df_filtered["VN_MANAGEMENT_ENTITY_NAME"].isin(filters["management_entity"])]
    
    # Separate Dropdowns for Venue Type, Global Type, and Pay Type
    filters["venue_type"] = st.sidebar.multiselect("Select Venue Type", df_filtered["VN_VENUE_TYPE_NAME"].unique(), default=filters.get("venue_type", []))
    if filters["venue_type"]:
        df_filtered = df_filtered[df_filtered["VN_VENUE_TYPE_NAME"].isin(filters["venue_type"])]
    
    filters["global_type"] = st.sidebar.multiselect("Select Global Type", df_filtered["FB_GLOBALTYPE_DESC"].unique(), default=filters.get("global_type", []))
    if filters["global_type"]:
        df_filtered = df_filtered[df_filtered["FB_GLOBALTYPE_DESC"].isin(filters["global_type"])]
    
    filters["pay_type"] = st.sidebar.multiselect("Select Pay Type", df_filtered["FB_PAYTYPE_DESC"].unique(), default=filters.get("pay_type", []))
    if filters["pay_type"]:
        df_filtered = df_filtered[df_filtered["FB_PAYTYPE_DESC"].isin(filters["pay_type"])]
    
    filters["venue"] = st.sidebar.multiselect("Select Venue", df_filtered["VN_VENUE_NAME"].unique(), default=filters.get("venue", []))
    if filters["venue"]:
        df_filtered = df_filtered[df_filtered["VN_VENUE_NAME"].isin(filters["venue"])]
    
    filters["pay_status"] = st.sidebar.multiselect("Select Pay Status", df_filtered["FB_PAYACTION_DESC"].unique(), default=filters.get("pay_status", []))
    if filters["pay_status"]:
        df_filtered = df_filtered[df_filtered["FB_PAYACTION_DESC"].isin(filters["pay_status"])]
    
    # Save updated filters back to data_store
    ds.save_filters(filters)

    if df_filtered.empty:
        st.error("No data available with the current filters. Please select different filters.")
    else:
        # Extract Day of the Week
        df_filtered['DayOfWeek'] = df_filtered['FB_CREATESERVICETSTAMP'].dt.day_name()
        
        # Group by Day of the Week and YearMonth
        df_grouped_dow = df_filtered.groupby(['DayOfWeek', df_filtered['FB_CREATESERVICETSTAMP'].dt.to_period('M').astype(str)]).agg({
            'FB_CHARGE_AMOUNT': 'sum',
            'FB_SPENDAGREE_AMOUNT': 'sum',
            'FB_SUBTOTAL_AMOUNT': 'sum',
            'FB_PLANNED_GUEST_COUNT': 'sum'
        }).reset_index().rename(columns={'FB_CREATESERVICETSTAMP': 'YearMonth'})
        
        # Sort by Day of the Week
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df_grouped_dow['DayOfWeek'] = pd.Categorical(df_grouped_dow['DayOfWeek'], categories=day_order, ordered=True)
        df_grouped_dow = df_grouped_dow.sort_values(['YearMonth', 'DayOfWeek'])
        
        # Create separate charts for each metric
        fig_charge_amount = px.line(df_grouped_dow, x='YearMonth', y='FB_CHARGE_AMOUNT', color='DayOfWeek',
                                    title="CHARGE_AMOUNT by Day of the Week Over Time", markers=True)
        
        fig_spendagree_amount = px.line(df_grouped_dow, x='YearMonth', y='FB_SPENDAGREE_AMOUNT', color='DayOfWeek',
                                        title="SPENDAGREE_AMOUNT by Day of the Week Over Time", markers=True)
        
        fig_subtotal_amount = px.line(df_grouped_dow, x='YearMonth', y='FB_SUBTOTAL_AMOUNT', color='DayOfWeek',
                                      title="SUBTOTAL_AMOUNT by Day of the Week Over Time", markers=True)
        
        fig_planned_guest_count = px.line(df_grouped_dow, x='YearMonth', y='FB_PLANNED_GUEST_COUNT', color='DayOfWeek',
                                          title="PLANNED_GUEST_COUNT by Day of the Week Over Time", markers=True)
        
        # Display charts in the tab
        with trend_chart_tab:
            st.plotly_chart(fig_charge_amount, use_container_width=True)
            st.plotly_chart(fig_spendagree_amount, use_container_width=True)
            st.plotly_chart(fig_subtotal_amount, use_container_width=True)
            st.plotly_chart(fig_planned_guest_count, use_container_width=True)
        
        # Display data frame in the tab
        with trend_dataframe_tab:
            st.write("Transaction Data by Day of the Week Over Time")
            st.dataframe(df_grouped_dow, height=400, width=1000)
else:
    st.error("Failed to retrieve data.")