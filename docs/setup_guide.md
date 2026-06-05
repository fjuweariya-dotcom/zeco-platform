\# ZECO Intelligent Energy Analytics Platform - Setup Guide



\## Prerequisites



\* Docker Desktop

\* Git

\* Python 3.12+

\* Apache Spark 3.5.3

\* Jupyter Notebook



\---



\## Start All Services



```bash

docker compose up -d

```



Verify running containers:



```bash

docker ps

```



\---



\## Access Services



| Service             | URL                   |

| ------------------- | --------------------- |

| Spark Master        | http://localhost:8080 |

| Jupyter Notebook    | http://localhost:8888 |

| MinIO Console       | http://localhost:9001 |

| PostgreSQL          | localhost:5432        |

| Kafka               | localhost:9092        |

| Prometheus          | http://localhost:9090 |

| Grafana             | http://localhost:3000 |

| Airflow             | http://localhost:8085 |

| Streamlit Dashboard | http://localhost:8501 |



\---



\## Data Pipeline Workflow



1\. Historical electricity transactions are ingested.

2\. Kafka streams transaction events.

3\. Spark Structured Streaming processes incoming data.

4\. Bronze layer stores raw records.

5\. Silver layer stores cleaned and validated records.

6\. Gold layer stores business analytics tables.

7\. Machine learning models generate forecasts and anomaly alerts.

8\. Streamlit dashboards visualize insights.



\---



\## Machine Learning Modules



\### Revenue Protection



\* Isolation Forest anomaly detection

\* Fraud indicator generation

\* Suspicious customer identification



\### Forecasting



\* ARIMA Revenue Forecasting

\* Prophet Token Forecasting



\---



\## Workflow Orchestration



Airflow DAG:



```text

dags/zeco\_pipeline\_dag.py

```



Automated Tasks:



\* Revenue Protection Analysis

\* ARIMA Forecast Generation

\* Prophet Forecast Generation

\* Gold Layer Refresh



\---



\## Project Documentation



```text

docs/

├── Project Proposal

├── Progress Report

├── Setup Guide

└── Dashboard Screenshots

```



