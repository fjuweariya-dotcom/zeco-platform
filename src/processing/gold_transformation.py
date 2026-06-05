from config.spark_session import get_spark
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg,
    count,
    month,
    year,
    max as spark_max,
    min as spark_min,
    stddev,
    when,
    datediff,
    current_date
)


def main():
    spark = get_spark("ZECO-Gold-Transformation")

    silver_path = "s3a://lakehouse/zeco/silver/customers_clean/"
    gold_base_path = "s3a://lakehouse/zeco/gold"

    print("Reading Silver data from:", silver_path)
    df = spark.read.parquet(silver_path)

    print("Creating Gold analytics tables...")

    monthly_revenue = (
        df.withColumn("year", year(col("transaction_date")))
          .withColumn("month", month(col("transaction_date")))
          .groupBy("year", "month")
          .agg(
              spark_sum("purchase_amount").alias("total_revenue"),
              spark_sum("electricity_units").alias("total_units_sold"),
              count("*").alias("total_transactions"),
              avg("purchase_amount").alias("avg_purchase_amount")
          )
    )

    town_summary = (
        df.groupBy("town")
          .agg(
              spark_sum("purchase_amount").alias("total_revenue"),
              spark_sum("electricity_units").alias("total_units_sold"),
              count("*").alias("total_transactions"),
              avg("purchase_amount").alias("avg_purchase_amount")
          )
    )

    customer_summary = (
        df.groupBy("meter_number", "account_number")
          .agg(
              spark_sum("purchase_amount").alias("customer_total_spent"),
              spark_sum("electricity_units").alias("customer_total_units"),
              count("*").alias("customer_transactions"),
              avg("purchase_amount").alias("customer_avg_purchase"),
              spark_max("transaction_date").alias("last_purchase_date"),
              spark_min("transaction_date").alias("first_purchase_date")
          )
    )

    payment_trends = (
        df.withColumn("year", year(col("transaction_date")))
          .withColumn("month", month(col("transaction_date")))
          .groupBy("year", "month", "town")
          .agg(
              spark_sum("purchase_amount").alias("monthly_town_revenue"),
              count("*").alias("monthly_town_transactions"),
              avg("purchase_amount").alias("avg_payment_value")
          )
    )

    fraud_base = (
        df.groupBy("meter_number", "account_number", "town")
          .agg(
              count("*").alias("transaction_count"),
              spark_sum("purchase_amount").alias("total_spent"),
              avg("purchase_amount").alias("avg_purchase"),
              stddev("purchase_amount").alias("std_purchase"),
              spark_max("purchase_amount").alias("max_purchase"),
              spark_min("purchase_amount").alias("min_purchase"),
              spark_sum("electricity_units").alias("total_units"),
              spark_max("transaction_date").alias("last_transaction_date")
          )
    )

    percentile_95 = fraud_base.selectExpr(
        "percentile(transaction_count, 0.95)"
    ).collect()[0][0]

    print(f"Transaction count 95th percentile: {percentile_95}")

    fraud_anomaly_indicators = (
        fraud_base
          .withColumn(
              "high_value_flag",
              when(col("max_purchase") > 100000, 1).otherwise(0)
          )
          .withColumn(
              "frequent_transaction_flag",
              when(col("transaction_count") > percentile_95, 1).otherwise(0)
          )
          .withColumn(
              "inactive_customer_flag",
              when(datediff(current_date(), col("last_transaction_date")) > 90, 1).otherwise(0)
          )
          .withColumn(
              "anomaly_score",
              col("high_value_flag") + col("frequent_transaction_flag")
          )
    )

    print("Writing monthly revenue...")
    monthly_revenue.coalesce(1).write.mode("overwrite").parquet(
        f"{gold_base_path}/monthly_revenue/"
    )

    print("Writing town summary...")
    town_summary.repartition(20).write.mode("overwrite").parquet(
        f"{gold_base_path}/town_summary/"
    )

    print("Writing customer summary...")
    customer_summary.repartition(50).write.mode("overwrite").parquet(
        f"{gold_base_path}/customer_summary/"
    )

    print("Writing payment trends...")
    payment_trends.repartition(30).write.mode("overwrite").parquet(
        f"{gold_base_path}/payment_trends/"
    )

    print("Writing fraud anomaly indicators...")
    fraud_anomaly_indicators.repartition(50).write.mode("overwrite").parquet(
        f"{gold_base_path}/fraud_anomaly_indicators/"
    )

    print("Extended Gold transformation complete.")
    print("Check MinIO: lakehouse/zeco/gold")


if __name__ == "__main__":
    main()