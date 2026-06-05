from sklearn.preprocessing import StandardScaler
from config.spark_session import get_spark
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, stddev,
    max as spark_max, min as spark_min,
    datediff, to_date, lit, when
)
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest


def main():
    spark = get_spark("ZECO_REVENUE_PROTECTION_ANOMALY")

    silver_path = "s3a://lakehouse/zeco/silver/customers_clean/"
    output_path = "s3a://lakehouse/zeco/gold/fraud_anomaly_indicators/"
    local_csv = "D:/zeco-platform/data/processed/revenue_protection_anomalies.csv"

    print("Reading Silver data...")
    df = spark.read.parquet(silver_path)
    df = df.withColumn("purchase_date", to_date(col("transaction_date")))

    latest_date = df.agg(spark_max("purchase_date")).collect()[0][0]
    print("Latest transaction date:", latest_date)

    # ---------------------------------------------------
    # Base customer behavior features
    # ---------------------------------------------------
    base = (
        df.groupBy("meter_number", "account_number", "town")
          .agg(
              count("*").alias("transaction_count"),
              spark_sum("purchase_amount").alias("total_spent"),
              avg("purchase_amount").alias("avg_purchase"),
              spark_max("purchase_amount").alias("max_purchase"),
              stddev("purchase_amount").alias("std_purchase"),

              spark_sum("electricity_units").alias("total_units"),
              avg("electricity_units").alias("avg_units"),
              spark_max("electricity_units").alias("max_units"),
              stddev("electricity_units").alias("std_units"),

              spark_min("purchase_date").alias("first_purchase_date"),
              spark_max("purchase_date").alias("last_transaction_date")
          )
          .withColumn(
              "customer_lifetime_days",
              datediff(col("last_transaction_date"), col("first_purchase_date")) + lit(1)
          )
          .withColumn(
              "avg_depletion_interval_days",
              col("customer_lifetime_days") / col("transaction_count")
          )
          .withColumn(
              "inactive_days",
              datediff(lit(latest_date), col("last_transaction_date"))
          )
    )

    # ---------------------------------------------------
    # Recent vs historical consumption behavior
    # ---------------------------------------------------
    recent = (
        df.filter(datediff(lit(latest_date), col("purchase_date")) <= 30)
          .groupBy("meter_number")
          .agg(
              avg("electricity_units").alias("recent_avg_units"),
              avg("purchase_amount").alias("recent_avg_purchase"),
              count("*").alias("recent_transaction_count")
          )
    )

    historical = (
        df.filter(datediff(lit(latest_date), col("purchase_date")) > 30)
          .groupBy("meter_number")
          .agg(
              avg("electricity_units").alias("historical_avg_units"),
              avg("purchase_amount").alias("historical_avg_purchase"),
              count("*").alias("historical_transaction_count")
          )
    )

    features = (
        base.join(recent, on="meter_number", how="left")
            .join(historical, on="meter_number", how="left")
            .fillna({
                "std_purchase": 0,
                "std_units": 0,
                "recent_avg_units": 0,
                "recent_avg_purchase": 0,
                "recent_transaction_count": 0,
                "historical_avg_units": 0,
                "historical_avg_purchase": 0,
                "historical_transaction_count": 0
            })
            .withColumn(
                "consumption_drop_ratio",
                when(
                    col("historical_avg_units") > 0,
                    col("recent_avg_units") / col("historical_avg_units")
                ).otherwise(1)
            )
            .withColumn(
                "purchase_spike_ratio",
                when(
                    col("avg_purchase") > 0,
                    col("max_purchase") / col("avg_purchase")
                ).otherwise(1)
            )
            .withColumn(
                "unit_spike_ratio",
                when(
                    col("avg_units") > 0,
                    col("max_units") / col("avg_units")
                ).otherwise(1)
            )
    )

    print("Converting to Pandas for Isolation Forest...")
    pdf = features.toPandas()

    numeric_cols = [
        "transaction_count",
        "total_spent",
        "avg_purchase",
        "max_purchase",
        "std_purchase",
        "total_units",
        "avg_units",
        "max_units",
        "std_units",
        "customer_lifetime_days",
        "avg_depletion_interval_days",
        "inactive_days",
        "recent_avg_units",
        "recent_avg_purchase",
        "recent_transaction_count",
        "historical_avg_units",
        "historical_avg_purchase",
        "historical_transaction_count",
        "consumption_drop_ratio",
        "purchase_spike_ratio",
        "unit_spike_ratio"
    ]

    X = pdf[numeric_cols].replace([np.inf, -np.inf], np.nan).fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    print("Training Isolation Forest anomaly model...")
    model = IsolationForest(
        n_estimators=200,
        contamination=0.08,
        random_state=42
    )


    model.fit(X_scaled)

    pdf["isolation_prediction"] = model.predict(X_scaled)
    pdf["isolation_score"] = model.decision_function(X_scaled)
    pdf["risk_score"] = -pdf["isolation_score"]

    # ---------------------------------------------------
    # Convert ML risk into dashboard-friendly categories
    # ---------------------------------------------------
    p90 = pdf["risk_score"].quantile(0.90)
    p98 = pdf["risk_score"].quantile(0.98)

    pdf["anomaly_score"] = 0
    pdf.loc[pdf["risk_score"] >= p90, "anomaly_score"] = 1
    pdf.loc[pdf["risk_score"] >= p98, "anomaly_score"] = 2

    # ---------------------------------------------------
    # Explainable revenue-protection scenario flags
    # ---------------------------------------------------
    pdf["irregular_vending_volume_flag"] = (
        pdf["max_purchase"] >= pdf["max_purchase"].quantile(0.95)
    ).astype(int)

    pdf["frequent_transaction_flag"] = (
        pdf["transaction_count"] >= pdf["transaction_count"].quantile(0.95)
    ).astype(int)

    pdf["unusual_depletion_interval_flag"] = (
        pdf["avg_depletion_interval_days"] <= pdf["avg_depletion_interval_days"].quantile(0.05)
    ).astype(int)

    pdf["sudden_consumption_drop_flag"] = (
        pdf["consumption_drop_ratio"] < 0.30
    ).astype(int)

    pdf["purchase_spike_flag"] = (
        pdf["purchase_spike_ratio"] >= 3
    ).astype(int)

    pdf["unit_spike_flag"] = (
        pdf["unit_spike_ratio"] >= 3
    ).astype(int)

    pdf["inactive_customer_flag"] = (
        pdf["inactive_days"] > 90
    ).astype(int)

    pdf["high_variability_flag"] = (
        pdf["std_purchase"] >= pdf["std_purchase"].quantile(0.95)
    ).astype(int)

    # Keep old column name for dashboard compatibility
    pdf["high_value_flag"] = pdf["irregular_vending_volume_flag"]

    pdf["fraud_scenario_score"] = (
        pdf["irregular_vending_volume_flag"]
        + pdf["frequent_transaction_flag"]
        + pdf["unusual_depletion_interval_flag"]
        + pdf["sudden_consumption_drop_flag"]
        + pdf["purchase_spike_flag"]
        + pdf["unit_spike_flag"]
        + pdf["inactive_customer_flag"]
        + pdf["high_variability_flag"]
    )

    print("\n===== New Anomaly Score Distribution =====")
    print(pdf["anomaly_score"].value_counts().sort_index())

    print("\n===== Scenario Flag Counts =====")
    scenario_cols = [
        "irregular_vending_volume_flag",
        "frequent_transaction_flag",
        "unusual_depletion_interval_flag",
        "sudden_consumption_drop_flag",
        "purchase_spike_flag",
        "unit_spike_flag",
        "inactive_customer_flag",
        "high_variability_flag"
    ]

    print(pdf[scenario_cols].sum())

    print("Saving local CSV...")
    pdf.to_csv(local_csv, index=False)

    print("Reading CSV back with Spark...")
    result = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(local_csv)
    )

    print("Writing updated fraud anomaly indicators to Gold...")
    result.write.mode("overwrite").parquet(output_path)

    print("Revenue protection anomaly detection complete.")
    print("Output:", output_path)

    spark.stop()


if __name__ == "__main__":
    main()