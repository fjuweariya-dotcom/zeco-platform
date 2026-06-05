import os
import pandas as pd
from config.spark_session import get_spark
from pyspark.sql.functions import (
    col, sum as spark_sum, count, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


INPUT_DIR = "D:/zeco-platform/data/streaming/input"
OUTPUT_DIR = "D:/zeco-platform/data/processed/live"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_live_outputs(batch_df, batch_id):
    print(f"Processing streaming batch: {batch_id}")

    if batch_df.count() == 0:
        return

    kpis = (
        batch_df
        .agg(
            spark_sum(col("purchase_amount").cast("double")).alias("live_revenue"),
            spark_sum(col("electricity_units").cast("double")).alias("live_units_sold"),
            count("*").alias("live_transactions")
        )
        .withColumn("last_updated", current_timestamp())
        .toPandas()
    )

    kpis.to_csv(
        f"{OUTPUT_DIR}/live_kpis.csv",
        index=False
    )

    town_activity = (
        batch_df
        .groupBy("town")
        .agg(
            spark_sum(col("purchase_amount").cast("double")).alias("live_town_revenue"),
            count("*").alias("live_town_transactions")
        )
        .orderBy(col("live_town_revenue").desc())
        .limit(20)
        .toPandas()
    )

    town_activity.to_csv(
        f"{OUTPUT_DIR}/live_town_activity.csv",
        index=False
    )

    alerts = (
        batch_df
        .filter(col("purchase_amount").cast("double") > 100000)
        .select(
            "meter_number",
            "account_number",
            "town",
            "purchase_amount",
            "electricity_units"
        )
        .limit(50)
        .toPandas()
    )

    alerts.to_csv(
        f"{OUTPUT_DIR}/live_alerts.csv",
        index=False
    )

    print("Live KPI, town activity, and alerts updated.")


def main():
    spark = get_spark("ZECO_LIVE_STREAM_PROCESSOR")

    schema = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_DIR)
        .schema
    )

    stream_df = (
        spark.readStream
        .option("header", True)
        .schema(schema)
        .csv(INPUT_DIR)
    )

    query = (
        stream_df.writeStream
        .foreachBatch(write_live_outputs)
        .outputMode("append")
        .option("checkpointLocation", "D:/zeco-platform/data/streaming/checkpoint")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("ZECO live stream processor started.")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()


if __name__ == "__main__":
    main()