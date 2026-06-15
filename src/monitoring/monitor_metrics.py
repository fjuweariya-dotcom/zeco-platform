import subprocess
import time
import os

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_kafka_lag():
    """Get Kafka topic size"""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'zeco-kafka', 'kafka-run-class', 
             'kafka.tools.GetOffsetShell', '--bootstrap-server', 'localhost:9092',
             '--topic', 'vending-transactions', '--time', '-1'],
            capture_output=True, text=True
        )
        offsets = result.stdout.strip().split('\n')
        total = sum(int(offset.split(':')[2]) for offset in offsets if offset)
        return total
    except:
        return 0

def get_bronze_count():
    """Get records in bronze layer"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format("delta").load("s3a://lakehouse-bronze/transactions/")
        return df.count()
    except:
        return 0

def monitor_dashboard():
    """Simple text dashboard"""
    print("🚀 MONITORING DASHBOARD (Press Ctrl+C to stop)\n")
    
    while True:
        clear_screen()
        print("="*60)
        print(f"📊 FRAUD DETECTION PIPELINE MONITOR")
        print(f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # Kafka status
        kafka_msgs = get_kafka_lag()
        print(f"\n📡 KAFKA TOPIC: vending-transactions")
        print(f"   Messages in Kafka: {kafka_msgs:,}")
        
        # Bronze layer
        bronze = get_bronze_count()
        print(f"\n🥉 BRONZE LAYER (Raw Data)")
        print(f"   Records ingested: {bronze:,}")
        
        if bronze > 0:
            progress = (bronze / 43000000) * 100
            print(f"   Progress: {progress:.2f}%")
            print(f"   Remaining: {43000000 - bronze:,}")
            
            # Rate calculation
            eta_minutes = ((43000000 - bronze) / (bronze / 10)) if bronze > 10 else 0
            print(f"   ETA: {eta_minutes:.1f} minutes")
        
        # System resources
        cpu = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        print(f"\n💻 SYSTEM RESOURCES")
        print(f"   CPU: {cpu:.1f}%")
        print(f"   Memory: {memory.percent:.1f}% ({memory.used/1e9:.1f}GB/{memory.total/1e9:.1f}GB)")
        
        print("\n" + "="*60)
        time.sleep(5)

if __name__ == "__main__":
    import psutil
    monitor_dashboard()