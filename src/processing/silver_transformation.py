from config.spark_session import get_spark
from pyspark.sql.functions import (
    col,
    to_timestamp,
    regexp_replace,
    trim,
    upper,
    current_timestamp
)


def main():
    spark = get_spark("ZECO-Silver-Transformation")

    #bronze_path = "s3a://zeco-raw/customers/"
    #silver_path = "s3a://zeco-processed/customers_clean/"

    bronze_path = "s3a://lakehouse/zeco/bronze/customers/"
    silver_path = "s3a://lakehouse/zeco/silver/customers_clean/"

    print("Reading Bronze data from:", bronze_path)

    df = spark.read.parquet(bronze_path)

    print("Bronze rows:", df.count())

    print("Cleaning and transforming data...")

    df_clean = (
        df
        .withColumn("transaction_date", to_timestamp(col("transaction_date")))
        .withColumn("customer_connectdate", to_timestamp(col("customer_connectdate")))

        .withColumn("purchase_amount", col("purchase_amount").cast("double"))
        .withColumn("electricity_units", col("electricity_units").cast("double"))
        .withColumn("Tax_amount", col("Tax_amount").cast("double"))
        .withColumn("tariff_charge", col("tariff_charge").cast("double"))
        .withColumn("fixed_charge", col("fixed_charge").cast("double"))
        .withColumn("arrears_paid", col("arrears_paid").cast("double"))
        .withColumn("outstanding_amount", col("outstanding_amount").cast("double"))

        .withColumn("vendor_id", col("vendor_id").cast("int"))
        .withColumn("meter_number", col("meter_number").cast("long"))
        .withColumn("receipt_number", col("receipt_number").cast("long"))

        .withColumn("account_number", trim(col("account_number")))
        .withColumn("consumer_surname", upper(trim(col("consumer_surname"))))
        .withColumn("consumer_first_name", upper(trim(col("consumer_first_name"))))
        .withColumn("town", upper(trim(col("town"))))
        .withColumn("vending_category", trim(col("vending_category")))

        .withColumn("ingestion_timestamp", current_timestamp())
        .dropDuplicates(["receipt_number"])
    )

    print("Silver transformation ready. Writing to MinIO...")

    print("Writing Silver data to:", silver_path)

    (
        df_clean
        .repartition(100)
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print("Silver transformation complete. Check MinIO: zeco-processed/customers_clean")


if __name__ == "__main__":
    main()