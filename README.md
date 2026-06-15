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
2. Start infrastructure (Kafka, MinIO, Prometheus, Grafana)
bash
docker-compose up -d
3. Initialise Delta tables
bash
python scripts/init_delta_tables.py
4. Run the pipeline
Terminal 1 – Kafka producer (sends CSV data)

bash
python src/ingestion/kafka_producer.py data/raw/big1-data.csv
Terminal 2 – Streaming ingestion (writes to Bronze)

bash
python src/ingestion/streaming_ingestion.py
Terminal 3 – Bronze → Silver (after enough data is ingested)

bash
python src/processing/bronze_to_silver.py
Terminal 4 – Silver → Gold (feature engineering)

bash
python src/processing/silver_to_gold.py
Terminal 5 – Fraud detection (rules + Isolation Forest)

bash
python src/ml/isolation_forest.py
⏱️ For 48 million records, expect ~30‑60 min per batch step.

📊 Generate Synthetic Data
If you don't have real data, generate synthetic test data:

bash
# Generate 10,000 normal records
python scripts/generate_synthetic_data.py --records 10000 --customers 100

# Generate 1 million records
python scripts/generate_synthetic_data.py --records 1000000 --customers 500

# Generate fraud test scenario
python scripts/generate_synthetic_data.py --scenario fraud_high_spend
📈 Monitoring & Dashboards
Prometheus metrics endpoint
Spark exposes metrics at:
http://localhost:4041/metrics/prometheus

Grafana
URL: http://localhost:3000

Login: admin / admin

Adding a dashboard
Add Prometheus data source: http://prometheus:9090

Import dashboard or create your own panels.

Useful PromQL queries:

Metric	Query
Input rate (records/sec)	rate(ingested_records_total[1m])
Processed rows/sec	spark_streaming_processedRowsPerSecond
Batch duration (p95)	spark_streaming_batchDuration_seconds{quantile="0.95"}
Total customers	total_customers
Total revenue	total_revenue
Customer Metrics Dashboard
The customer_metrics.py script collects business metrics and stores them in PostgreSQL for Grafana dashboards:

bash
# Run once
python monitoring/customer_metrics.py --mode once

# Run continuously every 5 minutes
python monitoring/customer_metrics.py --mode continuous --interval 5
🔧 Configuration
docker-compose.yml services
Service	Ports (host)	Purpose
Zookeeper	2181	Kafka coordination
Kafka	9092	Message broker
MinIO	9000, 9001	S3‑compatible object store
PostgreSQL	5432	Metrics database
Redis	6379	Caching
Prometheus	9090	Metrics collection
Grafana	3000	Visualisation
Environment variables
Set in your shell or .env file:

MINIO_ROOT_USER, MINIO_ROOT_PASSWORD (default: minioadmin)

KAFKA_BOOTSTRAP_SERVERS (default: localhost:9092)

POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

🧪 Testing with Sample Data
If you don't have a full CSV, generate test data:

bash
python scripts/generate_synthetic_data.py --records 10000 --customers 100
Or produce random messages directly to Kafka:

python
from kafka import KafkaProducer
import json, random, time
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode())
while True:
    producer.send('vending-transactions', {
        'meter_id': random.randint(1,100),
        'purchase_amount': random.uniform(10,500)
    })
    time.sleep(0.1)
🛠️ Troubleshooting
OutOfMemoryError in Spark
Reduce maxOffsetsPerTrigger to 200000 or 100000.

Increase spark.driver.memory and spark.executor.memory to 12g if you have enough RAM.

ClassNotFoundException: S3AFileSystem
Ensure the Spark session includes:

python
.config("spark.jars.packages", 
        "io.delta:delta-spark_2.12:3.1.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262")
Prometheus shows no metrics
Verify Spark streaming job is running.

Check http://localhost:4041/metrics/prometheus is reachable.

In prometheus.yml, use host.docker.internal:4041 (not localhost).

Restart Prometheus: docker-compose restart prometheus.

Container conflicts
bash
# Remove existing containers
docker rm -f zeco-kafka zeco-zookeeper zeco-minio zeco-postgres

# Then restart
docker-compose up -d
Kafka consumer lag
Monitor with:

bash
docker exec zeco-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group streaming-ingestion --describe
Grafana “No data”
Confirm Prometheus data source works.

Run a query like {job="spark_streaming"} in Prometheus Graph.

Ensure the Grafana panel time range matches the data.

📊 Data Schema (17 columns)
Column	Type	Description
transaction_date	TIMESTAMP	Date/time of transaction
vendor_id	STRING	Vendor identifier
meter_number	STRING	Meter identifier
account_number	STRING	Customer account number
receipt_number	STRING	Unique receipt number
purchase_amount	DOUBLE	Amount paid
electricity_units	DOUBLE	kWh purchased
tax_amount	DOUBLE	Tax charged
tariff_charge	DOUBLE	Tariff fee
fixed_charge	DOUBLE	Fixed service fee
arrears_paid	DOUBLE	Previous debt paid
consumer_surname	STRING	Customer last name
consumer_first_name	STRING	Customer first name
customer_connectedate	STRING	Connection date
vending_category	STRING	Customer category
town	STRING	Location
outstanding_amount	DOUBLE	Remaining debt
📚 Additional Documentation
Delta Lake

Spark Structured Streaming

MinIO

Prometheus + Grafana

🤝 Contributing
Fork the repository.

Create a feature branch (git checkout -b feature/amazing-feature).

Commit your changes.

Push to the branch.

Open a Pull Request.

📄 License
This project is licensed under the MIT License – see the LICENSE file for details.

✨ Acknowledgements
Built with Apache Spark, Delta Lake, Kafka, MinIO, Prometheus & Grafana.

Inspired by real‑world fraud detection in utility prepaid metering.

For questions or issues, please open a GitHub issue or contact the maintainer.

🎯 Key Features
✅ Real-time data ingestion from Kafka

✅ Delta Lake for ACID transactions and time travel

✅ Bronze → Silver → Gold medallion architecture

✅ Rule-based fraud detection (3 business rules)

✅ Isolation Forest unsupervised ML

✅ Prometheus + Grafana monitoring

✅ PostgreSQL for business metrics

✅ Docker Compose for easy deployment

✅ Synthetic data generator for testing

text

---

## 📥 How to Save

1. **Create the README file** in your project root:
   ```bash
   touch README.md
Copy the entire content above and paste it into README.md

Save the file

Add to Git (optional):

bash
git add README.md
git commit -m "Add comprehensive README for ZECO fraud detection platform"
git push



