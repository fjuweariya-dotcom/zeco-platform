from config.spark_session import get_spark
import pandas as pd
import numpy as np

from statsmodels.tsa.arima.model import ARIMA

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error
)

spark = get_spark("ZECO_ARIMA_SELECTION")

path = "s3a://lakehouse/zeco/gold/arima_daily_features/"

print("Loading data...")

df = (
    spark.read.parquet(path)
         .select("date", "daily_revenue")
         .orderBy("date")
)

pdf = df.toPandas()

pdf["date"] = pd.to_datetime(pdf["date"])
pdf = pdf.set_index("date")

series = pdf["daily_revenue"]

# last 30 days reserved for testing
train = series[:-30]
test = series[-30:]

models = [
    (1, 1, 1),
    (2, 1, 2),
    (3, 1, 3),
    (5, 1, 2),
    (7, 1, 7)
]

results = []

for order in models:

    print(f"\nTesting ARIMA{order}")

    try:

        model = ARIMA(train, order=order)

        fitted = model.fit()

        forecast = fitted.forecast(len(test))

        mae = mean_absolute_error(test, forecast)

        rmse = np.sqrt(
            mean_squared_error(test, forecast)
        )

        mape = (
            np.mean(
                np.abs(
                    (test - forecast) / test
                )
            ) * 100
        )

        results.append({
            "order": order,
            "AIC": fitted.aic,
            "BIC": fitted.bic,
            "MAE": mae,
            "RMSE": rmse,
            "MAPE": mape
        })

    except Exception as e:

        print("FAILED:", e)

results_df = pd.DataFrame(results)

print("\n==============================")
print("ARIMA MODEL COMPARISON")
print("==============================")

print(
    results_df.sort_values("RMSE")
)

spark.stop()