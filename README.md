# ZECO Intelligent Energy Analytics Platform

## Architecture

![Architecture](docs/dashboard_screenshots/architecture_diagram.png)

## Features

- Real-Time Streaming Analytics
- Revenue Protection & Fraud Detection
- ARIMA Revenue Forecasting
- Prophet Token Forecasting
- Airflow Workflow Automation
- Spark Lakehouse Architecture
- Streamlit Business Dashboards

## Dashboard Screenshots

### KPI Dashboard

![KPI](docs/dashboard_screenshots/kpi_dashboard.png)

### Live Streaming Analytics

![Live Streaming](docs/dashboard_screenshots/live_streaming.png)

### Revenue Analytics

![Revenue Analytics](docs/dashboard_screenshots/revenue_analytics.png)

### Town Analytics

![Town Analytics](docs/dashboard_screenshots/town_analytics.png)

### Fraud & Anomaly Dashboard

![Fraud Dashboard](docs/dashboard_screenshots/Fraud%20%26%20Anomaly.png)

### Fraud Indicators

![Fraud Indicators](docs/dashboard_screenshots/Fraud_Indicator.png)

### Suspicious Customer Explorer

![Suspicious Customers](docs/dashboard_screenshots/suspicious_customers.png)

### Prophet Forecast

![Forecast](docs/dashboard_screenshots/prophet_forecast.png)

## Machine Learning Components

### Revenue Protection

- Isolation Forest anomaly detection
- Risk scoring framework
- Fraud indicator generation
- Revenue-at-risk estimation

### Forecasting

- ARIMA Revenue Forecasting
- Prophet Token Depletion Forecasting

## Workflow Orchestration

Apache Airflow DAG:

```text
dags/zeco_pipeline_dag.py
```

Automated workflows:

1. Revenue Protection Anomaly Detection
2. ARIMA Forecast Generation
3. Prophet Forecast Generation
4. Gold Layer Analytics Refresh

## Documentation

Project reports are available in:

```text
docs/
├── Project Proposal
├── Progress Report
└── Dashboard Screenshots
```
