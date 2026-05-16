import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
import shutil
from datetime import datetime


BASE_DIR=os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR =os.path.join(BASE_DIR,'output')

def setup_output_dir():
    meta_dir = os.path.join(OUTPUT_DIR, 'metadata_inspection')
    if os.path.exists(meta_dir):
        shutil.rmtree(meta_dir)
    os.makedirs(meta_dir,exist_ok=True)
    return meta_dir

def create_sample_dataset(num_rows=100000):
    print("Creating sample dataset...")

    data={
        'transaction_id':np.arange(1,num_rows+1),
        'user_id': np.random.randint(1000, 10000, num_rows),
        'amount': np.random.uniform(1, 10000, num_rows),
        'timestamp':pd.date_range('2023-01-01',periods=num_rows,freq='h'),
        'category':np.random.choice(
            ['Electronics', 'Clothing', 'Food', 'Books', 'Home'],
            num_rows
        ),
        'is_refunded': np.random.choice([True, False], num_rows),
        'payment_method': np.random.choice(
            ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer'],
            num_rows
        )
    }

    df=pd.DataFrame(data)

    print(f"Created dataset: {len(df):,} rows × {len(df.columns)} columns")
    return df

def write_parquet_multi_rowgroup(df, output_path, rows_per_group=10000):
    

    print(f"\nWriting Parquet file with row groups (~{rows_per_group} rows each)...")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        row_group_size=rows_per_group
    )

    print(f" Written to {output_path}")
    print(f"  File size: {os.path.getsize(output_path) / (1024**2):.2f} MB")
    
    return output_path



def inspect_file_metadata(parquet_file):
    print("\n" + "=" * 60)
    print("File-Level Metadata")
    print("=" * 60)

    parquet_file_obj=pq.ParquetFile(parquet_file)


    print(f"\nFile: {os.path.basename(parquet_file)}")
    print(f"  Size: {os.path.getsize(parquet_file) / (1024**2):.2f} MB")

    metadata=parquet_file_obj.metadata
    
    print(f"\nTable Metadata:")
    print(f"  Total rows: {metadata.num_rows:,}")
    print(f"  Row groups: {metadata.num_row_groups}")
    print(f"  Compressed size: {metadata.serialized_size} bytes")

    schema=parquet_file_obj.schema
    print(f"\nSchema:")
    print(f"  Columns: {len(schema)}")

    for i ,col in enumerate(schema):
        print(f"    {i+1}. {col.name}: {col.physical_type}")
    return parquet_file_obj,metadata,schema



def inspect_column_statistics(parquet_file_obj):
    
    print("\n" + "=" * 60)
    print("Column Statistics (from metadata, no data read)")
    print("=" * 60)

    metadata = parquet_file_obj.metadata

    for rg_idx in range(metadata.num_row_groups):
        row_group=metadata.row_group(rg_idx)
        
        print(f"\nRow Group {rg_idx}:")
        print(f"  Rows: {row_group.num_rows:,}")
        print(f"  Bytes: {row_group.total_byte_size:,}")

        for col_idx in range(row_group.num_columns):
            col = row_group.column(col_idx)
            col_name=parquet_file_obj.schema.column(col_idx).name

            print(f"\n  {col_name}:")
            print(f"    Type: {col.physical_type}")
            print(f"    Encodings: {col.encodings}")
            print(f"    Compressed size: {col.total_compressed_size:,} bytes")
            print(f"    Uncompressed size: {col.total_uncompressed_size:,} bytes")

            

            if col.is_stats_set:
                stats = col.statistics
                print(f"    Statistics available: ")
                if hasattr(stats,'min') and hasattr(stats,'max'):
                    try:
                        print(f"      Min: {stats.min}")
                        print(f"      Max: {stats.max}")
                    except:
                        print(f"      Min/Max: [binary data]")
                else:
                    print(f"    Statistics available: ")


def get_column_statistics_summary(parquet_file_obj):
    print("\n" + "=" * 60)
    print("Column Statistics Summary")
    print("=" * 60)
    
    metadata=parquet_file_obj.metadata
    schema=parquet_file_obj.schema

    stats_summary = {}

    for col_idx in range(len(schema)):
        col_name=schema.column(col_idx).name
        col_type=schema.column(col_idx).physical_type

        min_vals=[]
        max_vals=[]

        for rg_idx in range(metadata.num_row_groups):
            row_group = metadata.row_group(rg_idx)
            col=row_group.column(col_idx)

            
















def main():
    print("=" * 60)
    print("Parquet Recipe 5: Metadata Inspection")
    print("=" * 60)


    meta_dir = setup_output_dir()

    df=create_sample_dataset(num_rows=100000)
    df = df.sort_values(by='amount')

    parquet_file=os.path.join(meta_dir,'transactions.parquet')

    write_parquet_multi_rowgroup(df, parquet_file)

    parquet_file_obj = pq.ParquetFile(parquet_file)

    inspect_file_metadata(parquet_file)
    inspect_column_statistics(parquet_file_obj)
    get_column_statistics_summary(parquet_file_obj)















if __name__ == '__main__':
    main()