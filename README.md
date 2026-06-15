markdown
# ZECO Fraud Detection Platform

A real‑time fraud detection system for prepaid electricity vending transactions. The pipeline ingests CSV data through Kafka, stores raw data in a Delta Lake (Bronze layer), cleans and enriches it (Silver layer), aggregates features (Gold layer), and applies rule‑based + unsupervised machine learning (Isolation Forest) to flag suspicious meters.

---

## 🏗️ Architecture Overview
<img width="1188" height="732" alt="architecture" src="https://github.com/user-attachments/assets/90c3882d-e821-4150-b039-bc9a9f22f33e" />


---

## 📦 Project Structure
zeco-platform/
├── data/raw/
│ └── big1-data.csv # Input CSV (17 columns)
├── scripts/
│ ├── init_delta_tables.py # Create Bronze/Silver/Gold tables
│ ├── clear_all_data.py # Reset all data (MinIO + metastore)
│ └── generate_synthetic_data.py # Generate synthetic test data
├── src/
│ ├── ingestion/
│ │ ├── kafka_producer.py # Sends CSV to Kafka (unlimited)
│ │ └── streaming_ingestion.py # Kafka → Bronze (structured streaming)
│ ├── processing/
│ │ ├── bronze_to_silver.py # Clean & enrich → Silver
│ │ └── silver_to_gold.py # Feature aggregation → Gold
│ └── ml/
│ └── isolation_forest.py # Combined rule + ML fraud detection
├── docker-compose.yml # Kafka, Zookeeper, MinIO
├── requirements.txt # Python dependencies
└── README.md

text

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (with at least 8 GB memory)
- Python 3.9+ virtual environment

### 1. Setup environment

```bash
cd zeco-platform
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate          # Windows
pip install -r requirements.txt
2. Start infrastructure
bash
docker-compose up -d
3. Initialise Delta tables
bash
python scripts/init_delta_tables.py
4. Run the pipeline
Terminal 1 – Kafka producer

bash
python src/ingestion/kafka_producer.py data/raw/big1-data.csv
Terminal 2 – Streaming ingestion

bash
python src/ingestion/streaming_ingestion.py
Terminal 3 – Bronze → Silver

bash
python src/processing/bronze_to_silver.py
Terminal 4 – Silver → Gold

bash
python src/processing/silver_to_gold.py
Terminal 5 – Fraud detection

bash
python src/ml/isolation_forest.py
⏱️ For 48 million records, expect ~30‑60 min per batch step.

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
🔧 Pipeline Stages
Bronze Layer (Raw Data)
Ingests data from Kafka in real-time

Stores raw transactions exactly as received

Adds metadata: ingestion_timestamp, batch_id, source_file, data_quality_status

Silver Layer (Cleaned Data)
Removes duplicates based on receipt_number

Filters out negative purchase amounts

Adds derived columns: year, month, day_of_week, is_weekend, price_per_unit

Gold Layer (Feature Engineering)
Aggregates per meter

Creates features: total_transactions, total_spent, avg_purchase_amount, spending_volatility, vendor_loyalty, etc.

Calculates rule‑based fraud risk score

Fraud Detection
Rule‑based: Never bought, sharp decline (>50%), inactive >6 months

ML‑based: Isolation Forest on numeric features

Combines both for final fraud flag

📊 Generate Synthetic Data
If you don't have real data:

bash
# Generate 10,000 normal records
python scripts/generate_synthetic_data.py --records 10000 --customers 100

# Generate 1 million records
python scripts/generate_synthetic_data.py --records 1000000 --customers 500
🔍 Fraud Detection Rules
Rule	Description	Flag
Never bought	total_units == 0 or total_transactions == 0	rule_never_bought
Sharp decline	Recent avg < 50% of historical avg	rule_sharp_decline
Inactive	No purchase for > 180 days	rule_inactive_6months
Rule score: 0-3 (sum of above flags)

Fraud risk categories:

HIGH → risk score > 0.7

MEDIUM → risk score > 0.4

LOW → risk score ≤ 0.4

🧪 Testing with Sample Data
Generate random test messages to Kafka:

python
from kafka import KafkaProducer
import json, random, time

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode())
while True:
    producer.send('vending-transactions', {
        'transaction_date': '2024-01-01 10:00:00',
        'meter_number': f'MTR_{random.randint(1,100)}',
        'purchase_amount': random.uniform(10,500),
        'electricity_units': random.uniform(5,250),
        'receipt_number': f'RCP_{random.randint(1000,9999)}'
    })
    time.sleep(0.1)
🛠️ Troubleshooting
OutOfMemoryError in Spark
Reduce maxOffsetsPerTrigger in streaming_ingestion.py:

python
.option("maxOffsetsPerTrigger", "100000")   # Reduce from 500000
ClassNotFoundException: S3AFileSystem
Ensure Spark session includes Hadoop AWS packages:

python
.config("spark.jars.packages", 
        "io.delta:delta-spark_2.12:3.1.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262")
Container conflicts
bash
# Remove existing containers
docker rm -f zeco-kafka zeco-zookeeper zeco-minio

# Restart
docker-compose up -d
Clear all data (fresh start)
bash
python scripts/clear_all_data.py
docker-compose down -v
docker-compose up -d
python scripts/init_delta_tables.py
Schema mismatch error
Drop the existing table and recreate:

python
spark.sql("DROP TABLE IF EXISTS zeco.bronze_transactions")
Then restart the streaming ingestion.

📈 Performance Estimates
Stage	1M records	10M records	48M records
Kafka Producer	2-3 min	20-30 min	2-3 hours
Streaming (Bronze)	3-5 min	30-40 min	3-4 hours
Bronze → Silver	5-10 min	30-60 min	2-3 hours
Silver → Gold	10-15 min	1-2 hours	3-4 hours
🔧 Configuration
Spark memory settings
In streaming_ingestion.py:

python
.config("spark.driver.memory", "8g")
.config("spark.executor.memory", "8g")
.config("spark.sql.shuffle.partitions", "800")
Kafka settings
In docker-compose.yml:

yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
📚 Additional Documentation
Delta Lake

Spark Structured Streaming

MinIO

📄 License
This project is licensed under the MIT License.

✨ Acknowledgements
Built with Apache Spark, Delta Lake, Kafka, and MinIO.

text

---

This README reflects the **core fraud detection pipeline** as it existed before today's monitoring and dashboard additions. It includes:

- ✅ Complete pipeline architecture
- ✅ All 17 columns schema
- ✅ Step-by-step execution
- ✅ Fraud detection rules
- ✅ Troubleshooting guide
- ✅ Performance estimates

Save this as `README.md` in your project root.
