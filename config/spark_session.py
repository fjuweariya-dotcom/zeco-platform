import os
from pyspark.sql import SparkSession

# Explicit Python executable for Spark workers
PYTHON_EXE = r"D:\zeco-platform\venv312\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE


def get_spark(app_name="ZECO"):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # Local JARs already downloaded
        .config(
            "spark.jars",
            "file:///D:/zeco-platform/jars/hadoop-aws-3.3.4.jar,,"
            "file:///D:/zeco-platform/jars/aws-java-sdk-bundle-1.12.262.jar"
        )

        # Force Python 3.12
        .config("spark.pyspark.python", PYTHON_EXE)
        .config("spark.pyspark.driver.python", PYTHON_EXE)

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	.config("spark.hadoop.fs.s3a.buffer.dir", "D:/spark-temp/s3a")
	.config("spark.hadoop.fs.s3a.fast.upload", "true")
	.config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
	.config("spark.hadoop.fs.s3a.committer.name", "directory")
	.config("spark.hadoop.fs.s3a.multipart.size", "64M")

        # Temp directories
        .config("spark.local.dir", "D:/spark-temp")
        .config("spark.hadoop.tmp.dir", "D:/spark-temp")

        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")

        .getOrCreate()
    )

    return spark