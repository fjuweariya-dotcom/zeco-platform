import os
import streamlit as st
import pandas as pd
import plotly.express as px

from config.spark_session import get_spark

st.set_page_config(
    page_title="ZECO Intelligent Energy Analytics",
    layout="wide"
)

st.title("  ZECO Intelligent Energy Analytics Platform")


# ---------------------------------------------------
# Spark Session
# ---------------------------------------------------

@st.cache_resource
def load_spark():
    return get_spark("ZECO-Dashboard")

spark = load_spark()

# ---------------------------------------------------
# LOAD GOLD TABLES
# ---------------------------------------------------

@st.cache_data
def load_data():

    monthly_revenue = spark.read.parquet(
        "s3a://lakehouse/zeco/gold/monthly_revenue/"
    ).toPandas()

    town_summary = spark.read.parquet(
        "s3a://lakehouse/zeco/gold/town_summary/"
    ).toPandas()

    customer_summary = spark.read.parquet(
        "s3a://lakehouse/zeco/gold/customer_summary"
    ).toPandas()

    fraud_df = spark.read.parquet(
        "s3a://lakehouse/zeco/gold/fraud_anomaly_indicators/"
        
    ).toPandas()

    payment_trends = spark.read.parquet(
        "s3a://lakehouse/zeco/gold/payment_trends/"

    ).toPandas()

    return (
        monthly_revenue,
        town_summary,
        customer_summary,
        fraud_df,
        payment_trends
    )

(
    monthly_revenue,
    town_summary,
    customer_summary,
    fraud_df,
    payment_trends
) = load_data()


#st.subheader("DEBUG: Monthly Revenue Data")

#st.write(
 #   monthly_revenue[
  #      ["year", "month", "total_revenue", "total_transactions"]
   # ].tail(10)
#)

# ---------------------------------------------------
# KPI SECTION
# ---------------------------------------------------

st.header("  KPI Overview")

total_revenue = monthly_revenue["total_revenue"].sum()
total_transactions = monthly_revenue["total_transactions"].sum()
total_units = monthly_revenue["total_units_sold"].sum()
total_anomalies = fraud_df[fraud_df["anomaly_score"] > 0].shape[0]

potential_revenue_at_risk = (
    fraud_df[
        fraud_df["anomaly_score"] > 0
    ]["total_spent"]
    .sum()
)

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Revenue", f"{total_revenue:,.0f}")
col2.metric("Total Transactions", f"{total_transactions:,.0f}")
col3.metric("Electricity Units Sold", f"{total_units:,.0f}")
col4.metric("Anomaly Cases", f"{total_anomalies:,}")
with col5:
    st.metric(
        "Potential Revenue At Risk",
        f"TZS {potential_revenue_at_risk:,.0f}"
    )

# ---------------------------------------------------
# LIVE STREAMING ANALYTICS
# ---------------------------------------------------

st.header("  Live Streaming Analytics")

LIVE_DIR = "data/processed/live"

try:
    live_kpis = pd.read_csv(f"{LIVE_DIR}/live_kpis.csv")

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Live Revenue",
        f"{live_kpis['live_revenue'][0]:,.0f}"
    )

    c2.metric(
        "Live Units Sold",
        f"{live_kpis['live_units_sold'][0]:,.0f}"
    )

    c3.metric(
        "Live Transactions",
        f"{live_kpis['live_transactions'][0]:,.0f}"
    )

except Exception:
    st.warning("Live KPI stream data is not available yet.")

try:
    live_alerts = pd.read_csv(f"{LIVE_DIR}/live_alerts.csv")
    live_alerts_count = len(live_alerts)

    st.warning(f"{live_alerts_count:,} Revenue Protection Alerts Generated"
)

    st.subheader("  Live Revenue Protection Alerts")

    st.dataframe(live_alerts.head(20))

except Exception:
    st.info("No live revenue protection alerts available yet.")

try:
    live_town = pd.read_csv(f"{LIVE_DIR}/live_town_activity.csv")

    fig_live_town = px.bar(
        live_town.head(15),
        x="town",
        y="live_town_revenue",
        text="live_town_transactions",
        title="Live Town Revenue Activity"
    )

    st.plotly_chart(
        fig_live_town,
        width="stretch",
        key="live_town_activity_chart"
    )

except Exception:
    st.info("Live town activity data is not available yet.")


# ---------------------------------------------------
# MONTHLY REVENUE
# ---------------------------------------------------

st.header("  Monthly Revenue Analytics")

monthly_revenue = monthly_revenue.dropna(subset=["year", "month"])

monthly_revenue["year"] = monthly_revenue["year"].astype(int)
monthly_revenue["month"] = monthly_revenue["month"].astype(int)

monthly_revenue["period"] = pd.to_datetime(
    monthly_revenue["year"].astype(str)
    + "-"
    + monthly_revenue["month"].astype(str).str.zfill(2)
    + "-01"
)

monthly_revenue = monthly_revenue.sort_values("period")

latest_period = monthly_revenue["period"].max()

monthly_revenue_plot = monthly_revenue[
    monthly_revenue["period"] != latest_period
]


fig_rev = px.line(
    monthly_revenue_plot,
    x="period",
    y="total_revenue",
    markers=True,
    title="Monthly Revenue Trend"
)

st.info("Latest incomplete month excluded from revenue trend chart.")

# st.plotly_chart(fig_rev, use_container_width=True)

# st.plotly_chart(fig_rev, width="stretch")

st.plotly_chart(
    fig_rev,
    width="stretch",
    key="monthly_revenue_chart"
)

# ---------------------------------------------------
# TOWN SUMMARY
# ---------------------------------------------------

st.header("  Town / Region Electricity Usage")

top_towns = town_summary[
    town_summary["town"].notna()
]

top_towns = top_towns[
    top_towns["town"] != "NULL"
]

top_towns = top_towns.sort_values(
    "total_revenue",
    ascending=False
).head(20)

fig_town = px.bar(
    top_towns,
    x="town",
    y="total_revenue",
    color="total_transactions",
    title="Top Revenue Towns"
)


st.plotly_chart(
    fig_town,
    width="stretch",
    key="town_chart"
)

# Cleaner explanation chart: electricity usage only
clean_towns = (
    town_summary[
        town_summary["town"].notna()
    ]
)

clean_towns = clean_towns[
    clean_towns["town"] != "NULL"
]

clean_towns = clean_towns.sort_values(
    "total_units_sold",
    ascending=False
).head(15)

fig_town_units = px.bar(
    clean_towns,
    x="town",
    y="total_units_sold",
    title="Top 15 Towns by Electricity Units Sold"
)

st.plotly_chart(
    fig_town_units,
    width="stretch",
    key="town_units_chart"
)


# st.plotly_chart(fig_town, use_container_width=True)

# st.plotly_chart(fig_town, width="stretch")


# ---------------------------------------------------
# CUSTOMER ANALYTICS
# ---------------------------------------------------

st.header("  Customer Spending Analysis")

top_customers = customer_summary.sort_values(
    "customer_total_spent",
    ascending=False
).head(20)

fig_customers = px.bar(
    top_customers,
    x="account_number",
    y="customer_total_spent",
    title="Top Spending Customers"
)

# st.plotly_chart(fig_customers, use_container_width=True)

# st.plotly_chart(fig_customers, width="stretch")

st.plotly_chart(
    fig_customers,
    width="stretch",
    key="customer_chart"
)

# ---------------------------------------------------
# FRAUD DASHBOARD
# ---------------------------------------------------

st.header("  Fraud & Anomaly Dashboard")

anomaly_counts = (
    fraud_df.groupby("anomaly_score")
    .size()
    .reset_index(name="count")
)

fig_anomaly = px.bar(
    anomaly_counts,
    x="anomaly_score",
    y="count",
    text="count",
    title="Revenue Protection Risk Score Distribution"
)

# st.plotly_chart(fig_anomaly, use_container_width=True)

# st.plotly_chart(fig_anomaly, width="stretch")

st.plotly_chart(
    fig_anomaly,
    width="stretch",
    key="anomaly_chart"
)


# ---------------------------------------------------
# FRAUD SCENARIO COUNTS
# ---------------------------------------------------

scenario_cols = [
    "irregular_vending_volume_flag",
    "frequent_transaction_flag",
    "unusual_depletion_interval_flag",
    "sudden_consumption_drop_flag",
    "purchase_spike_flag",
    "unit_spike_flag",
    "inactive_customer_flag",
    "high_variability_flag"
]

available_cols = [
    c for c in scenario_cols
    if c in fraud_df.columns
]

if available_cols:

    scenario_counts = (
        fraud_df[available_cols]
        .sum()
        .reset_index()
    )

    scenario_counts.columns = [
        "scenario",
        "count"
    ]

    fig_scenarios = px.bar(
        scenario_counts,
        x="scenario",
        y="count",
        title="Fraud Scenario Indicator Counts"
    )

    st.plotly_chart(
        fig_scenarios,
        width="stretch",
        key="fraud_scenario_chart"
    )

# ---------------------------------------------------
# TOP TOWNS BY SUSPICIOUS ACTIVITY
# ---------------------------------------------------

fraud_by_town = (
    fraud_df[
        (fraud_df["anomaly_score"] > 0)
        & (fraud_df["town"].notna())
    ]
    .groupby("town")
    .size()
    .reset_index(name="fraud_cases")
    .sort_values(
        "fraud_cases",
        ascending=False
    )
)

fig_fraud_town = px.bar(
    fraud_by_town.head(15),
    x="town",
    y="fraud_cases",
    text="fraud_cases",
    title="Top Towns by Suspicious Activity"
)

st.plotly_chart(
    fig_fraud_town,
    width="stretch",
    key="fraud_town_chart"
)


high_risk_count = len(
    fraud_df[fraud_df["anomaly_score"] == 2]
)

st.warning(f"""
Revenue Protection Alert

{high_risk_count:,} customers are currently classified as High Risk
and should be prioritized for investigation.
"""
)


# ---------------------------------------------------
# SUSPICIOUS CUSTOMERS
# ---------------------------------------------------


st.header("  Suspicious Customer Explorer")

suspicious_customers = fraud_df[
    fraud_df["anomaly_score"] > 0
].sort_values(
    "risk_score",
    ascending=False
)

st.info(
    """
Risk Score = weighted fraud risk indicator.

Anomaly Score:
0 = Normal
1 = Medium Risk
2 = High Risk

Fraud Scenario Score = number of fraud indicators triggered by a customer.
"""
)

st.dataframe(
    suspicious_customers[
        [
            "meter_number",
            "account_number",
            "town",
            "risk_score",
            "anomaly_score",
            "fraud_scenario_score",
            "transaction_count",
            "total_spent",
            "max_purchase",
            "sudden_consumption_drop_flag",
            "purchase_spike_flag",
            "inactive_customer_flag"
        ]
    ].head(50)
)

# ---------------------------------------------------
# PAYMENT TRENDS
# ---------------------------------------------------

#st.header("  Payment Trends")

#fig_payment = px.scatter(
    #payment_trends,
    #x="monthly_town_transactions",
    #y="monthly_town_revenue",
    #color="town",
    #size="avg_payment_value",
    #title="Town Payment Behavior"
#)

st.header("  Payment Trends")

# Bar chart version: clearer for presentation
payment_top = (
    payment_trends
    .groupby("town", as_index=False)
    .agg({
        "monthly_town_revenue": "sum",
        "monthly_town_transactions": "sum"
    })
    .sort_values("monthly_town_revenue", ascending=False)
    .head(15)
)

if not payment_top.empty:
    fig_payment_bar = px.bar(
        payment_top,
        x="town",
        y="monthly_town_revenue",
        color="monthly_town_transactions",
        title="Top 30 Town Monthly Payment Trends"
    )

    st.plotly_chart(
        fig_payment_bar,
        width="stretch",
        key="payment_bar_chart"
    )
else:
    st.warning("No payment trend data available.")

# Original scatter chart: keep for comparison
fig_payment = px.scatter(
    payment_trends,
    x="monthly_town_transactions",
    y="monthly_town_revenue",
    color="town",
    size="avg_payment_value",
    title="Town Payment Behavior"
)


# st.plotly_chart(fig_payment, use_container_width=True)

# st.plotly_chart(fig_payment, width="stretch")

#sst.plotly_chart(
#    fig_payment,
#    width="stretch",
#    key="payment_chart"
#)




# ---------------------------------------------------
# ARIMA FORECAST
# ---------------------------------------------------

st.header("  ARIMA Revenue Forecast")

arima_file = "data/processed/arima_forecast.csv"

if os.path.exists(arima_file):

    arima_df = pd.read_csv(arima_file)

    fig_arima = px.line(
        arima_df,
        x="date",
        y="forecast_revenue",
        markers=True,
        title="Next 30 Day Revenue Forecast"
    )

    st.plotly_chart(
        fig_arima,
        width="stretch",
        key="arima_chart"
    )
    
    st.dataframe(arima_df)

# ---------------------------------------------------
# TOKEN DEPLETION FORECAST
# ---------------------------------------------------

st.header("  Prophet Token Depletion Forecast")

prophet_file = "data/processed/prophet_meter_forecast.csv"

if os.path.exists(prophet_file):

    prophet_df = pd.read_csv(prophet_file)

    fig_prophet = px.line(
        prophet_df,
        x="ds",
        y="yhat",
        title="Next 30 Day Meter Consumption Forecast"
    )

    st.plotly_chart(
        fig_prophet,
        width="stretch",
        key="prophet_chart"
    )

    st.dataframe(prophet_df.tail(30))