markdown
# ZECO Fraud Detection Platform

A real‑time fraud detection system for prepaid electricity vending transactions. The pipeline ingests CSV data through Kafka, stores raw data in a Delta Lake (Bronze layer), cleans and enriches it (Silver layer), aggregates features (Gold layer), and applies rule‑based + unsupervised machine learning (Isolation Forest) to flag suspicious meters.

---

## 📥 Dataset Requirements

### You Need to Provide a CSV File

Before running the pipeline, **you must place your CSV file** in the `data/raw/` directory.

**Expected file location:** `data/raw/big1-data.csv`

**Required CSV format (17 columns):**

| Column | Type | Description |
|--------|------|-------------|
| transaction_date | TIMESTAMP | Date/time of transaction |
| vendor_id | STRING | Vendor identifier |
| meter_number | STRING | Meter identifier |
| account_number | STRING | Customer account number |
| receipt_number | STRING | Unique receipt number |
| purchase_amount | DOUBLE | Amount paid |
| electricity_units | DOUBLE | kWh purchased |
| tax_amount | DOUBLE | Tax charged |
| tariff_charge | DOUBLE | Tariff fee |
| fixed_charge | DOUBLE | Fixed service fee |
| arrears_paid | DOUBLE | Previous debt paid |
| consumer_surname | STRING | Customer last name |
| consumer_first_name | STRING | Customer first name |
| customer_connectedate | STRING | Connection date |
| vending_category | STRING | Customer category |
| town | STRING | Location |
| outstanding_amount | DOUBLE | Remaining debt |

> **Note:** The CSV must have a header row with these exact column names. If your CSV has different column names, update the schema in `src/ingestion/streaming_ingestion.py` accordingly.

### No Dataset? Generate Synthetic Data

If you don't have a real dataset, you can generate synthetic test data:

```bash
# Generate 10,000 synthetic records
python scripts/generate_synthetic_data.py --records 10000 --customers 100

# Generate 1 million records for testing
python scripts/generate_synthetic_data.py --records 1000000 --customers 500
This will create data/raw/synthetic_data.csv that you can use.

#🏗️ Architecture Overview
<img width="1188" height="732" alt="architecture" src="https://github.com/user-attachments/assets/900db45b-39db-4786-a86b-ee06bbdb9678" />

#📦 Project Structure
text
zeco-platform/
├── data/raw/
│   ├── big1-data.csv                    # ⚠️ YOU MUST PLACE YOUR CSV HERE
│   └── synthetic_data.csv               # Generated test data (optional)
├── scripts/
│   ├── init_delta_tables.py             # Create Bronze/Silver/Gold tables
│   ├── clear_all_data.py                # Reset all data (MinIO + metastore)
│   └── generate_synthetic_data.py       # Generate synthetic test data
├── src/
│   ├── ingestion/
│   │   ├── kafka_producer.py            # Sends CSV to Kafka (unlimited)
│   │   └── streaming_ingestion.py       # Kafka → Bronze (structured streaming)
│   ├── processing/
│   │   ├── bronze_to_silver.py          # Clean & enrich → Silver
│   │   └── silver_to_gold.py            # Feature aggregation → Gold
│   └── ml/
│       └── isolation_forest.py          # Combined rule + ML fraud detection
├── docker-compose.yml                   # Kafka, Zookeeper, MinIO
├── requirements.txt                     # Python dependencies
└── README.md
#🚀 Quick Start
Prerequisites
Docker Desktop (with at least 8 GB memory)

Python 3.9+ virtual environment

Your CSV file placed in data/raw/big1-data.csv

1. Setup environment
bash
cd zeco-platform
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate          # Windows
pip install -r requirements.txt
2. Place your CSV file
bash
# Create the data directory if it doesn't exist
mkdir -p data/raw

# Copy your CSV file to the required location
cp /path/to/your/data.csv data/raw/big1-data.csv
3. Start infrastructure
bash
docker-compose up -d
4. Initialise Delta tables
bash
python scripts/init_delta_tables.py
5. Run the pipeline
Terminal 1 – Kafka producer (sends your CSV to Kafka)

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
⏱️ Processing time depends on your dataset size. For 48 million records, expect ~30‑60 min per batch step.

🔧 Customizing for Your CSV
If your CSV has different column names
Edit the schema in src/ingestion/streaming_ingestion.py:

python
schema = StructType([
    StructField("your_date_column", StringType(), True),
    StructField("your_vendor_column", StringType(), True),
    # ... map all 17 columns to your actual column names
])
If your CSV has a different file name
Update the path when running the producer:

bash
python src/ingestion/kafka_producer.py data/raw/your_file_name.csv
🧪 Testing with Sample Data
If you want to test the pipeline without your real dataset:

bash
# Generate synthetic test data
python scripts/generate_synthetic_data.py --records 10000 --customers 100

# Run the producer with synthetic data
python src/ingestion/kafka_producer.py data/raw/synthetic_data.csv
🔍 Fraud Detection Rules
Rule	Description	Flag
Never bought	total_units == 0 or total_transactions == 0	rule_never_bought
Sharp decline	Recent avg < 50% of historical avg	rule_sharp_decline
Inactive	No purchase for > 180 days	rule_inactive_6months
Fraud risk categories:

HIGH → risk score > 0.7

MEDIUM → risk score > 0.4

LOW → risk score ≤ 0.4

🛠️ Troubleshooting
FileNotFoundError: data/raw/big1-data.csv
Solution: Place your CSV file in the data/raw/ directory, or generate synthetic data first.

OutOfMemoryError in Spark
Reduce maxOffsetsPerTrigger in streaming_ingestion.py:

python
.option("maxOffsetsPerTrigger", "100000")   # Reduce from 500000
ClassNotFoundException: S3AFileSystem
Ensure Spark session includes Hadoop AWS packages (already configured).

Container conflicts
bash
docker rm -f zeco-kafka zeco-zookeeper zeco-minio
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

📈 Performance Estimates (for 48M records)
Stage	Estimated Time
Kafka Producer	2-3 hours
Streaming (Bronze)	3-4 hours
Bronze → Silver	2-3 hours
Silver → Gold	3-4 hours
🔧 Configuration
Spark memory settings
In streaming_ingestion.py:

python
.config("spark.driver.memory", "8g")
.config("spark.executor.memory", "8g")
.config("spark.sql.shuffle.partitions", "800")
CSV file location
The default path is data/raw/big1-data.csv. You can change it by modifying the csv_path variable in src/ingestion/kafka_producer.py.

📚 Additional Documentation
Delta Lake

Spark Structured Streaming

MinIO

📄 License
This project is licensed under the MIT License.

✨ Acknowledgements
Built with Apache Spark, Delta Lake, Kafka, and MinIO.

text

**Key fixes made:**

1. Added proper code block backticks for the synthetic data generation command
2. Fixed the architecture image markdown syntax
3. Added proper code block for project structure
4. Fixed code block for troubleshooting sections
5. Added proper formatting for the performance estimates table
6. Ensured all bash commands are properly formatted
7. Fixed the Python code block indentation
8. Added back the missing closing backticks

Now the README is properly formatted and ready to use! 🎉
