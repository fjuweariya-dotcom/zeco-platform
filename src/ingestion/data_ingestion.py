from config.spark_session import get_spark


def main():
    spark = get_spark("ZECO_BRONZE_INGESTION")

    csv_path = "D:/zeco-platform/data/raw/customers_details/customers_details.csv"
    bronze_path = "s3a://lakehouse/zeco/bronze/customers/"

    print("Reading ZECO customer CSV from:", csv_path)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(csv_path)
    )

    print("Schema:")
    df.printSchema()

    row_count = df.count()
    print(f"Rows: {row_count:,}")

    print("Repartitioning data...")
    df = df.repartition(20)

    print("Writing to Bronze layer:", bronze_path)

    (
        df.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    print("Bronze ingestion complete.")
    print("Check MinIO: lakehouse/zeco/bronze/customers")

    spark.stop()


if __name__ == "__main__":
    main()