import os
import pandas as pd

INPUT_PATH = "data/raw/"
OUTPUT_PATH = "data/archive"

FILE_LIST = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
]

NUM_BATCHES = 5

for f in FILE_LIST:
    df = pd.read_csv(INPUT_PATH + f)
    chunk_size = (len(df) // NUM_BATCHES) + 1

    for i in range(NUM_BATCHES):
        start = i * chunk_size
        end = start + chunk_size
        split_df = df.iloc[start:end]
        batch_dir = f"{OUTPUT_PATH}/batch_{i+1}"
        os.makedirs(batch_dir, exist_ok=True)
        split_df.to_csv(f"{batch_dir}/{f}", index=False)
