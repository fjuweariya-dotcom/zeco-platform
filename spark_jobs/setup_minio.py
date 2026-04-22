"""Create MinIO bucket for data storage"""
from minio import Minio
import time

def create_buckets():
    """Create all required buckets in MinIO"""
    try:
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        
        buckets = [
            "zeco-data",
            "zeco-processed", 
            "zeco-anomalies",
            "zeco-metrics",
            "zeco-logs"
        ]
        
        for bucket in buckets:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"✓ Created bucket: {bucket}")
            else:
                print(f"✓ Bucket exists: {bucket}")
        
        print("\n✓ All buckets ready!")
        return True
        
    except Exception as e:
        print(f"✗ Error connecting to MinIO: {e}")
        print("Make sure MinIO is running: docker-compose up -d")
        return False

if __name__ == "__main__":
    create_buckets()