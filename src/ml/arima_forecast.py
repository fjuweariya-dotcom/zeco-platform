from config.spark_session import get_spark
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA


def main():

    spark = get_spark("ZECO_ARIMA_FORECAST")

    path = "s3a://lakehouse/zeco/gold/arima_daily_features/"
    local_output = "D:/zeco-platform/data/processed/arima_forecast.csv"

    print("Reading ARIMA features...")

    df = spark.read.parquet(path)

    pdf = (
        df.select("date", "daily_revenue")
          .orderBy("date")
          .toPandas()
    )

    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf = pdf.set_index("date")

    print("Training ARIMA(7,1,7)...")

    model = ARIMA(
        pdf["daily_revenue"],
        order=(7, 1, 7)
    )

    fitted = model.fit()

    forecast = fitted.forecast(30)

    forecast_df = forecast.reset_index()
    forecast_df.columns = ["date", "forecast_revenue"]

    print("\n===== NEXT 30 DAY REVENUE FORECAST =====")
    print(forecast_df)

    forecast_df.to_csv(local_output, index=False)

    print("ARIMA forecast saved locally:")
    print(local_output)

    spark.stop()


if __name__ == "__main__":
    main()