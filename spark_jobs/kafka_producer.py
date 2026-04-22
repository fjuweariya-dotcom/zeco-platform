"""
Ultra-fast Kafka Producer for historical data replay
Processing ALL 43M records - NO limits
"""

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import pandas as pd
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os
import signal
import sys

class FastHistoricalDataReplay:
    def __init__(self, bootstrap_servers='localhost:9092', topic='vending-transactions'):
        # Producer configuration - NO snappy compression
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=100,  # Batch messages
            batch_size=16384,  # 16KB batches
            buffer_memory=67108864,  # 64MB buffer (increased for 43M records)
            max_request_size=10485760,  # 10MB max request
            acks=1,  # Leader acknowledgment only (faster)
            retries=3,
            compression_type=None  # No compression to avoid dependency
        )
        self.topic = topic
        self.sent_count = 0
        self.start_time = None
        self.running = True
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        print(f"\n\n🛑 Received interrupt signal. Finishing current batch...")
        self.running = False
    
    def replay_bulk(self, csv_path, batch_size=50000):
        """
        Process ALL records - NO limits
        """
        # Check if file exists
        if not os.path.exists(csv_path):
            print(f"❌ File not found: {csv_path}")
            return 0
            
        print(f"📂 Loading CSV: {csv_path}")
        print(f"🎯 Target: Process ALL records (no limit)")
        
        # Get total records first (fast count)
        try:
            total_lines = sum(1 for _ in open(csv_path)) - 1  # Subtract header
            print(f"📊 Total records in file: {total_lines:,}")
        except:
            total_lines = "unknown"
        
        # Read CSV in chunks
        try:
            chunk_iter = pd.read_csv(csv_path, chunksize=batch_size)
        except Exception as e:
            print(f"❌ Error reading CSV: {e}")
            return 0
        
        total_sent = 0
        self.start_time = time.time()
        last_report_time = self.start_time
        
        print(f"\n🚀 Starting replay at MAXIMUM SPEED...")
        print(f"{'='*60}")
        
        for chunk_num, chunk in enumerate(chunk_iter):
            if not self.running:
                print(f"\n⚠️ Stopping at user request...")
                break
                
            # Process chunk
            messages = []
            for idx, row in chunk.iterrows():
                message = self._create_message(row, total_sent + idx)
                messages.append(message)
            
            # Send batch
            self._send_batch(messages)
            total_sent += len(messages)
            
            # Progress report every 5 seconds or 10 batches
            current_time = time.time()
            if current_time - last_report_time >= 5 or chunk_num % 10 == 0:
                elapsed = current_time - self.start_time
                rate = total_sent / elapsed if elapsed > 0 else 0
                remaining = total_lines - total_sent if isinstance(total_lines, int) else "unknown"
                
                print(f"[{elapsed:>8.1f}s] Batch {chunk_num:>6} | "
                      f"Sent: {total_sent:>10,} | "
                      f"Rate: {rate:>8,.0f} msg/sec | "
                      f"Remaining: {remaining if remaining == 'unknown' else f'{remaining:,}'}")
                last_report_time = current_time
        
        self.producer.flush()
        elapsed = time.time() - self.start_time
        final_rate = total_sent / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"✅ REPLAY COMPLETED!")
        print(f"   Total sent: {total_sent:,} records")
        print(f"   Total time: {elapsed:.2f} seconds ({elapsed/60:.1f} minutes)")
        print(f"   Avg rate: {final_rate:,.0f} msg/sec")
        print(f"{'='*60}")
        
        return total_sent
    
    def replay_parallel(self, csv_path, num_threads=4):
        """
        PARALLEL replay - Multiple threads for maximum throughput
        Processes ALL records
        """
        print(f"📂 Loading CSV with {num_threads} threads...")
        print(f"🎯 Target: Process ALL records (no limit)")
        
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            print(f"❌ Error reading CSV: {e}")
            return 0
        
        total_records = len(df)
        print(f"📊 Total records: {total_records:,}")
        
        chunk_size = total_records // num_threads
        
        # Split into chunks
        chunks = []
        for i in range(num_threads):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size if i < num_threads - 1 else total_records
            chunks.append(df.iloc[start_idx:end_idx])
        
        # Send in parallel
        self.start_time = time.time()
        print(f"\n🚀 Starting parallel replay with {num_threads} threads...")
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(self._send_dataframe, chunk) 
                      for chunk in chunks]
            results = [f.result() for f in futures]
        
        self.producer.flush()
        total_sent = sum(results)
        elapsed = time.time() - self.start_time
        
        print(f"\n{'='*60}")
        print(f"✅ PARALLEL REPLAY COMPLETED!")
        print(f"   Total sent: {total_sent:,} records")
        print(f"   Threads: {num_threads}")
        print(f"   Time: {elapsed:.2f} seconds ({elapsed/60:.1f} minutes)")
        print(f"   Rate: {total_sent/elapsed:,.0f} msg/sec")
        print(f"{'='*60}")
        
        return total_sent
    
    def _send_dataframe(self, df):
        """Send a dataframe chunk"""
        messages = []
        for idx, row in df.iterrows():
            message = self._create_message(row, idx)
            messages.append(message)
        
        self._send_batch(messages)
        return len(messages)
    
    def _create_message(self, row, idx):
        """Create message from CSV row"""
        # Handle various column name possibilities
        transaction_id = row.get('transaction_id')
        if pd.isna(transaction_id) or not transaction_id:
            transaction_id = f'TXN_{idx}_{int(time.time())}'
            
        meter_id = row.get('meter_id')
        if pd.isna(meter_id) or not meter_id:
            meter_id = f'METER_{idx}'
            
        # Get purchase amount (try different column names)
        purchase_amount = row.get('purchase_amount')
        if pd.isna(purchase_amount):
            purchase_amount = row.get('amount', 0)
        try:
            purchase_amount = float(purchase_amount)
        except:
            purchase_amount = 0.0
            
        # Get units purchased
        units = row.get('units_purchased')
        if pd.isna(units):
            units = row.get('units', 0)
        try:
            units = float(units)
        except:
            units = 0.0
        
        return {
            'transaction_id': str(transaction_id),
            'meter_id': str(meter_id),
            'token_id': str(row.get('token_id', f'TOKEN_{idx}')),
            'purchase_amount': purchase_amount,
            'units_purchased': units,
            'vendor_id': str(row.get('vendor_id', 'VENDOR_001')),
            'vending_point_id': str(row.get('vending_point_id', row.get('vending_point', 'VP_001'))),
            'transaction_date': self._parse_date(row.get('timestamp', row.get('date', row.get('transaction_date', datetime.now().isoformat())))),
            'payment_method': str(row.get('payment_method', 'CASH')),
            'customer_id': str(row.get('customer_id', f'CUST_{idx}')),
            'region': str(row.get('region', 'UNKNOWN'))
        }
    
    def _parse_date(self, date_value):
        """Parse date from various formats"""
        if pd.isna(date_value):
            return datetime.now().isoformat()
        try:
            return pd.to_datetime(date_value).isoformat()
        except:
            return datetime.now().isoformat()
    
    def _send_batch(self, messages):
        """Send batch of messages"""
        for msg in messages:
            self.producer.send(self.topic, value=msg)
    
    def close(self):
        self.producer.close()


# ============================================
# PROGRESS MONITOR
# ============================================

def monitor_progress():
    """Monitor Kafka topic for message count"""
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient
    
    try:
        consumer = KafkaConsumer(
            'vending-transactions',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )
        
        # Get end offsets
        partitions = consumer.partitions_for_topic('vending-transactions')
        end_offsets = consumer.end_offsets(partitions)
        total = sum(end_offsets.values())
        consumer.close()
        
        return total
    except:
        return 0


# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    import sys
    
    # Default CSV path
    csv_path = "data/raw/big1-data.csv"
    
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    print("="*60)
    print("🚀 KAFKA PRODUCER - FULL 43M RECORDS")
    print("="*60)
    print(f"📁 CSV Path: {csv_path}")
    print(f"📡 Kafka Topic: vending-transactions")
    print(f"🔧 Mode: NO LIMIT - Processing ALL records")
    print()
    
    # Check if CSV exists
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        print("\nPlease provide the correct path:")
        print(f"  python kafka_producer1.py /path/to/your/43M_records.csv")
        sys.exit(1)
    
    # Ask for replay mode
    print("Select replay mode:")
    print("  1. Bulk replay (faster, single thread)")
    print("  2. Parallel replay (fastest, multiple threads)")
    print("  3. Quick test (1000 records only)")
    
    choice = input("\nEnter choice (1, 2, or 3): ").strip()
    
    producer = FastHistoricalDataReplay()
    
    try:
        if choice == "1":
            # Bulk replay - NO limit
            producer.replay_bulk(csv_path, batch_size=50000)
            
        elif choice == "2":
            # Parallel replay - NO limit
            num_threads = int(input("Number of threads (default 4): ") or "4")
            producer.replay_parallel(csv_path, num_threads=num_threads)
            
        elif choice == "3":
            # Quick test - only 1000 records
            print("\n⚠️ Quick test mode: Only 1000 records")
            producer.replay_bulk(csv_path, batch_size=1000)
            
        else:
            print("Invalid choice. Running bulk replay...")
            producer.replay_bulk(csv_path, batch_size=50000)
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        producer.close()
        print("\n✅ Producer closed")
