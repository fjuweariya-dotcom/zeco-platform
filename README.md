# ZECO Fraud Detection Platform

A real‑time fraud detection system for prepaid electricity vending transactions. The pipeline ingests CSV data (or streams from Kafka), stores raw data in a Delta Lake (Bronze layer), cleans and enriches it (Silver layer), aggregates features (Gold layer), and finally applies rule‑based + unsupervised machine learning (Isolation Forest) to flag suspicious meters. All metrics are exposed to Prometheus and visualised in Grafana.

---

## 🏗️ Architecture Overview
<img width="1188" height="732" alt="architecture" src="https://github.com/user-attachments/assets/80aeb16a-9fed-4773-a27c-aadf7fe7acc4" />

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (with at least 8 GB memory)
- Python 3.9+ virtual environment
- Git (optional)

### 1. Clone & setup environment

```bash
git clone <your-repo-url>
cd zeco-platform
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate          # Windows
pip install -r requirements.txt
