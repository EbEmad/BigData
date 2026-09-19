import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import shutil
import time
from datetime import datetime

BASE_DIR=os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR =os.path.join(BASE_DIR,'output')


def setup_output_dir():

    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR,exist_ok=True)

    return OUTPUT_DIR

def create_sample_dataset(num_rows=100000):
    print("Creating sample dataset...")

    dates=pd.date_range('2023-01-01', periods=num_rows, freq='h')

    amount_base = 500 + 200 * np.sin(np.arange(num_rows) / 5000)
    amounts = amount_base + np.random.normal(0, 100, num_rows)
    amounts = np.clip(amounts, 10, 10000)

    data={
        'transaction_id': np.arange(1, num_rows + 1),
        'transaction_date': dates,
        'amount': amounts,
        'customer_id': np.random.randint(1000, 5000, num_rows),
        'category': np.random.choice(
            ['Electronics', 'Groceries', 'Clothing', 'Travel', 'Entertainment'],
            num_rows
        ),
        'status': np.random.choice(['completed', 'pending', 'cancelled'], num_rows)
    }

    df=pd.DataFrame(data)
    print(f" Created dataset: {len(df):,} rows × {len(df.columns)} columns")
    return df

def write_parquet_without_z_ordering(df, output_path, row_group_size=10000):
    print(f"\nWriting baseline Parquet file (no z-ordering)...")
    print(f"  Row group size: {row_group_size:,} rows")


    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    table = pa.Table.from_pandas(df)

    pq.write_table(
        table,
        output_path,
        row_group_size=row_group_size,
        compression='snappy'
    )

    file_size_mb = os.path.getsize(output_path) / (1024**2)
    print(f" Written to {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    return output_path


def compute_z_order_index(df, columns, bits=8):
    print(f"\nComputing z-order indices for columns: {columns}")
    
    z_indices = np.zeros(len(df), dtype=np.int64)
    max_val = (1 << bits) - 1  # 2^bits - 1

    for col_idx,col in enumerate(columns):
        col_data=df[col].values

        if col_data.dtype == 'datetime64[ns]':
            col_numeric = col_data.astype(np.int64)
        else:
            col_numeric=col_data.astype(np.float64)
        
        col_min=col_numeric.min()
        col_max=col_numeric.max()

        if col_max>col_min:
            normalized=((col_numeric-col_min)/(col_max-col_min)*max_val).astype(np.int64)
        else:
            normalized = np.zeros(len(col_data), dtype=np.int64)
        

        for bit_pos in range(bits):
            bit_mask = ((normalized >> bit_pos) & 1).astype(np.int64)
            z_indices |= bit_mask << (col_idx * bits + bit_pos)
        
        print(f"✓ Z-order indices computed")
        return z_indices
        



def write_parquet_with_z_ordering(df, output_path, z_columns, row_group_size=10000):
    print(f"\nWriting z-ordered Parquet file...")
    print(f"  Z-order columns: {z_columns}")
    print(f"  Row group size: {row_group_size:,} rows")

    z_indices = compute_z_order_index(df, z_columns)

    sorted_indices = np.argsort(z_indices)

    df_sorted = df.iloc[sorted_indices].reset_index(drop=True)

    os.makedirs(os.path.dirname(output_path),exist_ok=True)
    table = pa.Table.from_pandas(df_sorted)

    pq.write_table(
        table,
        output_path,
        row_group_size=row_group_size,
        compression='snappy'
    )


    file_size_mb = os.path.getsize(output_path) / (1024**2)
    print(f"✓ Written to {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    return output_path



def analyze_row_group_statistics(parquet_file, z_ordered=False):
    
    label = "Z-Ordered" if z_ordered else "Baseline"
    print(f"\n{'='*60}")
    print(f"{label} - Row Group Statistics")
    print(f"{'='*60}")


    pf = pq.ParquetFile(parquet_file)
    metadata=pf.metadata
    schema_arrow = pf.schema_arrow

    

    



    
def main():
    print("=" * 60)
    print("Parquet Recipe: Z-Ordering for Multi-Column Filtering")
    print("=" * 60)
    z_dir=setup_output_dir()

    df=create_sample_dataset(num_rows=100000)


    print(f"\nDataset Summary:")
    print(f"  Date range: {df['transaction_date'].min().date()} → {df['transaction_date'].max().date()}")
    print(f"  Amount range: ${df['amount'].min():.2f} → ${df['amount'].max():.2f}")
    print(f"  Mean amount: ${df['amount'].mean():.2f}")

    baseline_file = os.path.join(z_dir, 'transactions_baseline.parquet')
    write_parquet_without_z_ordering(df, baseline_file)

    z_ordered_file = os.path.join(z_dir, 'transactions_z_ordered.parquet')
    write_parquet_with_z_ordering(df, z_ordered_file, ['transaction_date', 'amount'])

    baseline_stats = analyze_row_group_statistics(baseline_file, z_ordered=False)


    

if __name__ == '__main__':
    main()