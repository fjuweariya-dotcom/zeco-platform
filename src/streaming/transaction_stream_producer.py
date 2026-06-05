import os
import time
import pandas as pd
from datetime import datetime

SOURCE_FILE = "D:/zeco-platform/data/raw/customers_details/customers_details.csv"
OUTPUT_DIR = "D:/zeco-platform/data/streaming/input"

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Reading sample transactions...")
df = pd.read_csv(SOURCE_FILE, nrows=5000)

batch_size = 100
batch_id = 0

print("Starting ZECO live transaction producer...")

for start in range(0, len(df), batch_size):
    batch = df.iloc[start:start + batch_size].copy()

    batch["stream_ingestion_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    output_file = os.path.join(
        OUTPUT_DIR,
        f"transactions_batch_{batch_id}.csv"
    )

    batch.to_csv(output_file, index=False)

    print(f"Wrote live batch {batch_id}: {output_file}")

    batch_id += 1
    time.sleep(5)

print("Live transaction producer finished.")