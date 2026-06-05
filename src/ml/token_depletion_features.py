from config.spark_session import get_spark
from pyspark.sql.functions import (
    col, to_date, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, datediff, lit
)


def main():
    spark = get_spark("ZECO_TOKEN_DEPLETION_FEATURES")

    silver_path = "s3a://lakehouse/zeco/silver/customers_clean/"
    output_path = "s3a://lakehouse/zeco/gold/token_depletion_features/"

    print("Reading Silver data...")
    df = spark.read.parquet(silver_path)

    df = df.withColumn("purchase_date", to_date(col("transaction_date")))

    meter_features = (
        df.groupBy("meter_number", "account_number", "town")
          .agg(
              count("*").alias("total_purchases"),
              spark_sum("electricity_units").alias("total_units_purchased"),
              spark_sum("purchase_amount").alias("total_amount_paid"),
              avg("electricity_units").alias("avg_units_per_purchase"),
              avg("purchase_amount").alias("avg_purchase_amount"),
              spark_min("purchase_date").alias("first_purchase_date"),
              spark_max("purchase_date").alias("last_purchase_date")
          )
          .withColumn(
              "customer_lifetime_days",
              datediff(col("last_purchase_date"), col("first_purchase_date")) + lit(1)
          )
          .withColumn(
              "avg_daily_units",
              col("total_units_purchased") / col("customer_lifetime_days")
          )
          .withColumn(
              "purchase_frequency_days",
              col("customer_lifetime_days") / col("total_purchases")
          )
          .withColumn(
              "estimated_days_to_depletion",
              col("avg_units_per_purchase") / col("avg_daily_units")
          )
    )

    print("Writing token depletion features...")
    meter_features.repartition(50).write.mode("overwrite").parquet(output_path)

    print("Token depletion feature table created.")
    print("Check MinIO: lakehouse/zeco/gold/token_depletion_features")

    spark.stop()


if __name__ == "__main__":
    main()