"""
Transform Bronze (Raw) to Silver (Cleaned) – 17‑column schema
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, year, month, dayofweek, to_timestamp
import time

class BronzeToSilver:
    def __init__(self):
        self.bronze_path = "s3a://lakehouse-bronze/transactions/"
        self.silver_path = "s3a://lakehouse-silver/cleaned_transactions/"

        self.spark = SparkSession.builder \
            .appName("ZecoBronzeToSilver") \
             .config("spark.jars.packages", 
                     "io.delta:delta-spark_2.12:3.1.0,"
                      "org.apache.hadoop:hadoop-aws:3.3.4,"
                      "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
              .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
              .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
              .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
              .config("spark.hadoop.fs.s3a.path.style.access", "true") \
              .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
              .config("spark.driver.memory", "8g") \
              .config("spark.executor.memory", "8g") \
              .config("spark.sql.shuffle.partitions", "800") \
              .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

    def deduplicate(self, df, id_column="receipt_number"):
        """Remove duplicates using hashing on receipt_number"""
        return df.dropDuplicates([id_column])

    def detect_reversals(self, df):
        """Flag negative purchase_amount as reversal"""
        return df.withColumn("is_reversal", when(col("purchase_amount") < 0, True).otherwise(False))

    def transform_batch(self):
        print("Reading from Bronze table...")
        bronze_df = self.spark.read.format("delta").load(self.bronze_path)

        total_bronze = bronze_df.count()
        print(f"Total records in Bronze: {total_bronze:,}")

        print("Removing duplicates...")
        deduped_df = self.deduplicate(bronze_df)
        after_dedup = deduped_df.count()
        print(f"Duplicates removed: {total_bronze - after_dedup:,}")

        print("Detecting reversals (filtering negative amounts)...")
        flagged_df = self.detect_reversals(deduped_df)

        # Keep only non‑reversal, positive purchase_amount
        silver_df = flagged_df.filter(col("is_reversal") == False) \
                              .filter(col("purchase_amount") > 0) \
                              .drop("is_reversal") \
                              .withColumn("layer", lit("silver"))

        # Add derived columns
        silver_df = silver_df \
            .withColumn("transaction_date", to_timestamp(col("transaction_date"))) \
            .withColumn("processing_date", current_timestamp()) \
            .withColumn("year", year("transaction_date")) \
            .withColumn("month", month("transaction_date")) \
            .withColumn("day_of_week", dayofweek("transaction_date")) \
            .withColumn("is_weekend", when(col("day_of_week").isin([1,7]), 1).otherwise(0)) \
            .withColumn("price_per_unit",
                        when(col("electricity_units") > 0,
                             col("purchase_amount") / col("electricity_units"))
                        .otherwise(0))

        silver_count = silver_df.count()
        print(f"Records after cleaning: {silver_count:,}")

        print(f"Writing {silver_count:,} records to Silver table...")
        silver_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(self.silver_path)

        # Create database and table reference
        self.spark.sql("CREATE DATABASE IF NOT EXISTS zeco")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS zeco.silver_transactions
            USING DELTA
            LOCATION '{self.silver_path}'
        """)

        print("Bronze to Silver transformation complete!")

if __name__ == "__main__":
    processor = BronzeToSilver()
    processor.transform_batch()