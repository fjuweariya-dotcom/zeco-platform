from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("verify_parquet").getOrCreate()

    df = spark.read.parquet("data/processed/zeco_transactions")

    print("ROWS:", df.count())
    df.printSchema()
    df.show(5)

if __name__ == "__main__":
    main()
