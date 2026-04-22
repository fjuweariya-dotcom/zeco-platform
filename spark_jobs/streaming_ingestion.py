"""
Streaming ingestion from Kafka to Delta Lake
Using existing MinIO buckets: lakehouse-bronze
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake and Kafka support for MinIO"""
    logger.info("Creating Spark session with MinIO, Delta Lake, and Kafka...")
    
    return SparkSession.builder \
        .appName("VendingStreamingIngestion") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://spark-checkpoints/") \
        .master("local[*]") \
        .getOrCreate()

def create_bronze_table(spark):
    """Create bronze table in lakehouse-bronze bucket"""
    logger.info("Setting up bronze table in lakehouse-bronze...")
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS zeco")
    
    # Check if table exists
    tables = spark.sql("SHOW TABLES IN zeco").filter(col("tableName") == "bronze_transactions").count()
    
    if tables == 0:
        logger.info("Creating bronze_transactions table...")
        
        # Create table with location in lakehouse-bronze - REMOVED problematic properties
        spark.sql("""
            CREATE TABLE zeco.bronze_transactions (
                transaction_id STRING,
                meter_id STRING,
                token_id STRING,
                purchase_amount DOUBLE,
                units_purchased DOUBLE,
                vendor_id STRING,
                vending_point_id STRING,
                transaction_date TIMESTAMP,
                payment_method STRING,
                customer_id STRING,
                region STRING,
                ingestion_timestamp TIMESTAMP,
                batch_id STRING,
                source_file STRING,
                data_quality_status STRING
            )
            USING DELTA
            LOCATION 's3a://lakehouse-bronze/transactions/'
        """)
        logger.info("✅ Bronze table created successfully")
    else:
        logger.info("✅ Bronze table already exists")
    
    return "zeco.bronze_transactions"

def ingest_stream():
    """Main streaming ingestion function"""
    logger.info("="*60)
    logger.info("Starting Vending Transaction Streaming Ingestion")
    logger.info("="*60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Define schema for incoming Kafka messages
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("meter_id", StringType(), True),
        StructField("token_id", StringType(), True),
        StructField("purchase_amount", DoubleType(), True),
        StructField("units_purchased", DoubleType(), True),
        StructField("vendor_id", StringType(), True),
        StructField("vending_point_id", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("payment_method", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True)
    ])
    
    logger.info("Reading from Kafka topic: vending-transactions")
    
    # Read stream from Kafka
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "vending-transactions") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("✅ Connected to Kafka successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise
    
    # Parse JSON messages
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("batch_id", expr("uuid()")) \
        .withColumn("source_file", lit("kafka")) \
        .withColumn("data_quality_status", lit("PENDING"))
    
    # Filter out nulls (empty messages)
    parsed_df = parsed_df.filter(col("transaction_id").isNotNull())
    
    # Create table if not exists
    create_bronze_table(spark)
    
    # Define paths
    bronze_path = "s3a://lakehouse-bronze/transactions/"
    checkpoint_path = "s3a://spark-checkpoints/streaming-ingestion/"
    
    logger.info(f"Output path: {bronze_path}")
    logger.info(f"Checkpoint path: {checkpoint_path}")
    
    # Write stream to Delta table
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="10 seconds") \
        .queryName("vending_stream_ingestion") \
        .start(bronze_path)
    
    logger.info("="*60)
    logger.info("✅ Streaming ingestion started successfully!")
    logger.info(f"   Writing to: {bronze_path}")
    logger.info(f"   Checkpoint: {checkpoint_path}")
    logger.info(f"   Kafka topic: vending-transactions")
    logger.info("   Press Ctrl+C to stop")
    logger.info("="*60)
    
    # Monitor progress
    while query.isActive:
        try:
            status = query.lastProgress
            if status and status.get('numInputRows', 0) > 0:
                logger.info(f"📊 Processed {status['numInputRows']} rows")
            time.sleep(30)
        except Exception as e:
            logger.debug(f"Progress check: {e}")
            break
    
    query.awaitTermination()

if __name__ == "__main__":
    try:
        ingest_stream()
    except KeyboardInterrupt:
        logger.info("\n🛑 Streaming stopped by user")
    except Exception as e:
        logger.error(f"❌ Streaming failed: {e}")
        raise