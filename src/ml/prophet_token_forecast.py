from config.spark_session import get_spark
from pyspark.sql.functions import col, to_date, count, sum as spark_sum
import pandas as pd
from prophet import Prophet


def main():
    spark = get_spark("ZECO_PROPHET_TOKEN_FORECAST")

    silver_path = "s3a://lakehouse/zeco/silver/customers_clean/"
    output_path = "s3a://lakehouse/zeco/gold/prophet_meter_forecast/"

    print("Reading Silver data...")
    df = spark.read.parquet(silver_path)

    # Select one active meter first for testing
    top_meter = (
        df.groupBy("meter_number")
          .agg(count("*").alias("txn_count"))
          .orderBy(col("txn_count").desc())
          .limit(1)
          .collect()[0]["meter_number"]
    )

    print("Selected meter:", top_meter)

    meter_daily = (
        df.filter(col("meter_number") == top_meter)
          .withColumn("ds", to_date(col("transaction_date")))
          .groupBy("ds")
          .agg(spark_sum("electricity_units").alias("y"))
          .filter(col("ds").isNotNull())
          .orderBy("ds")
    )

    pdf = meter_daily.toPandas()
    pdf["ds"] = pd.to_datetime(pdf["ds"])

    print("Training rows:", len(pdf))
    print(pdf.head())

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )

    model.fit(pdf)

    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)

    result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(30)

    print("\n===== NEXT 30 DAY METER-LEVEL TOKEN FORECAST =====")
    print(result)

    #spark_result = spark.createDataFrame(result)

    #spark_result.write.mode("overwrite").parquet(output_path)

    #print("Prophet meter forecast written to:")
    #print(output_path)

    #spark.stop()

    local_output = "D:/zeco-platform/data/processed/prophet_meter_forecast.csv"

    result.to_csv(local_output, index=False)

    print("Prophet meter forecast saved locally:")
    print(local_output)

    spark.stop()


if __name__ == "__main__":
    main()