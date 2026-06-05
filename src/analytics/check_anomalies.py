from config.spark_session import get_spark


def main():

    spark = get_spark("ZECO-Anomaly-Analytics")

    path = "s3a://zeco-curated/fraud_anomaly_indicators/"

    df = spark.read.parquet(path)

    print("Total records:")
    print(df.count())

    print("\nTotal anomaly cases:")
    anomalies = df.filter(df.anomaly_score > 0)

    print(anomalies.count())

    print("\nBreakdown by anomaly score:")
    df.groupBy("anomaly_score").count().show()

    print("\nTop suspicious customers:")
    anomalies.orderBy("anomaly_score", ascending=False).show(100, truncate=False)


if __name__ == "__main__":
    main()