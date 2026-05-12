import pandas as pd
import numpy as np
import time
import os
from datetime import datetime

BASE_DIR=os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR =os.path.join(BASE_DIR,'output')
os.makedirs(OUTPUT_DIR,exist_ok=True)
def create_wide_dataset(num_rows=50000, num_columns=100):
    print(f"Creating dataset with {num_rows} rows and {num_columns} columns...")
    
    data = {}

    data['id']=np.arange(num_rows)
    data['timestamp']=pd.date_range('2020-01-01',periods=num_rows,freq='h')
    data['user_id']=np.random.randint(1000,100000,num_rows)
    data['session_id']=np.random.randint(1,10000,num_rows)


    for i in range(num_columns - 4 - 10): 
        data[f'metric_{i:03d}']=np.random.randn(num_rows)*100
    
    for i in range(5):
        data[f'category_{i}']=np.random.choice(
            ['A','B','C','D','E'],
            num_rows
        )
    
    for i in range(5):
        data[f'flag_{i}']=np.random.choice([True,False],num_rows)
    
    df=pd.DataFrame(data)
    print(f" Created dataset: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"  Memory usage: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")
    
    return df
    
def wirte_parquet_file(df, filepath, compression='snappy'):
    print(f"\nWriting to {filepath}...")
    df.to_parquet(filepath, engine='pyarrow', compression=compression)

    return


def read_all_columns(filepath):
    print("\n" + "-" * 60)
    print("Scenario 1: Reading ALL columns")
    print("-" * 60)



    start_time=time.time()
    df=pd.read_parquet(filepath,engine='pyarrow')

    elapsed=time.time()-start_time


    memory_usage=df.memory_usage(deep=True).sum()/(1024**2)
    print(f" Read {len(df)} rows × {len(df.columns)} columns")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Memory loaded: {memory_usage:.2f} MB")
    
    return df, elapsed, memory_usage

def read_subset_columns(filepath, columns):
    print("\n" + "-" * 60)
    print(f"Scenario 2: Reading ONLY {len(columns)} columns")

    print(f"Columns: {', '.join(columns[:5])}" + (".." if len(columns)>5 else ""))

    print("-"*60)

    start_time=time.time()
    df = pd.read_parquet(filepath, columns=columns, engine='pyarrow')
    elapsed = time.time() - start_time

    memory_usage = df.memory_usage(deep=True).sum() / (1024**2)
    print(f" Read {len(df)} rows × {len(df.columns)} columns")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Memory loaded: {memory_usage:.2f} MB")
    
    return df, elapsed, memory_usage

def demonstrate_predicate_pushdown(filepath):
    print("\n" + "-" * 60)
    print("Scenario 4: With Predicate Pushdown (filtering during read)")
    print("-" * 60)
    
    filters=[('user_id','>',50000)]
    columns=['id','user_id','metric_000']

    start_time=time.time()

    df=pd.read_parquet(
        filepath,
        columns=columns,
        filters=filters,
        engine='pyarrow'
    )

    elapsed=time.time()-start_time

    memory_usage = df.memory_usage(deep=True).sum() / (1024**2)
    print(f" Read {len(df)} rows (after filtering) × {len(df.columns)} columns")
    print(f"  Applied filter: user_id > 50000")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Memory loaded: {memory_usage:.2f} MB")
    
    return df, elapsed, memory_usage



def main():
    print("=" * 60)
    print("Parquet Recipe 2: Column Selection Efficiency")
    print("=" * 60)
    
    df=create_wide_dataset(num_rows=50000, num_columns=100)
    parquet_file = os.path.join(OUTPUT_DIR, 'wide_dataset.parquet')
    wirte_parquet_file(df,parquet_file)


    all_columns=df.columns.tolist()

    df_all,time_all,mem_all=read_all_columns(parquet_file)

    some_cols=all_columns[:5]

    df_subset,time_subset,mem_subset=read_subset_columns(
        parquet_file,
        some_cols
    )

    df_filtered, time_filtered, mem_filtered=demonstrate_predicate_pushdown(
        parquet_file
    )

    print("\n" + "=" * 60)
    print("Performance Comparison Summary")
    print("=" * 60)

    print(f"  All columns (100):        {time_all:.3f}s (baseline)")

    print(f"  Some columns (5):        {time_subset:.3f}s ({(time_all/time_subset):.2f}x faster)")

    print(f"  With filter & 3 columns:  {time_filtered:.3f}s ({(time_all/time_filtered):.2f}x faster)")

    print("\nMemory Usage Comparison:")
    print(f"  All columns (100):        {mem_all:.2f} MB")
    print(f"  Essential columns (5):    {mem_subset:.2f} MB ({(mem_all/mem_subset):.1f}% reduction)")
    print(f"  With filter & 3 columns:  {mem_filtered:.2f} MB ({(mem_all/mem_filtered):.1f}% reduction)")





if __name__=='__main__':
    main()