from config.spark_session import get_spark
from pyspark.sql.functions import (
    col, to_date, sum as spark_sum, count, avg
)


def main():
    spark = get_spark("ZECO_ARIMA_FEATURES")

    silver_path = "s3a://lakehouse/zeco/silver/customers_clean/"
    output_path = "s3a://lakehouse/zeco/gold/arima_daily_features/"

    print("Reading Silver data...")
    df = spark.read.parquet(silver_path)

    daily_features = (
        df.withColumn("date", to_date(col("transaction_date")))
          .filter(col("date").isNotNull())
          .groupBy("date")
          .agg(
              spark_sum("purchase_amount").alias("daily_revenue"),
              spark_sum("electricity_units").alias("daily_units_sold"),
              count("*").alias("daily_transactions"),
              avg("purchase_amount").alias("avg_purchase_amount")
          )
          .orderBy("date")
    )

    print("Daily ARIMA feature rows:", daily_features.count())
    daily_features.show(10, truncate=False)

    print("Writing ARIMA daily features...")
    daily_features.coalesce(1).write.mode("overwrite").parquet(output_path)

    print("ARIMA daily feature table created.")
    print("Check MinIO: lakehouse/zeco/gold/arima_daily_features")

    spark.stop()


if __name__ == "__main__":
    main()