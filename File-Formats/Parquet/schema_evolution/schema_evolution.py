import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import shutil
from datetime import datetime, timedelta

BASE_DIR=os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR =os.path.join(BASE_DIR,'output')
schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('name', pa.string()),
    pa.field('email', pa.string()),
    pa.field('created_date', pa.date64()),
    pa.field('is_active', pa.bool_())
])


def setup_output_dir():
    schema_dir=os.path.join(OUTPUT_DIR,'schema_evolution')
    os.makedirs(schema_dir,exist_ok=True)
    return schema_dir

def version_1_original_schema(schema_dir,num_records=1000):
    print("=" * 60)
    print("Version 1: Original Schema")
    print("=" * 60)

    data = {
        'id': np.arange(num_records),
        'name': [f'user_{i}' for i in range(num_records)],
        'email': [f'user{i}@example.com' for i in range(num_records)],
        'created_date': pd.date_range('2023-01-01', periods=num_records, freq='h'),
        'is_active': np.random.choice([True, False], num_records)
    }

    df=pd.DataFrame(data)
    print("\n" + "-" * 60)
    print("Writing Version 1 file...")
    print("-" * 60)

    v1_dir = os.path.join(schema_dir, 'version_1')
    os.makedirs(v1_dir, exist_ok=True)
    
    file_path = os.path.join(v1_dir, 'version_1.parquet')

    df.to_parquet(
        file_path,
        engine='pyarrow',
        compression='snappy'
    )

    print(f"\n Written {file_path}")
    print(f"Schema:")

    for col in df.columns:
        print(f" -{col}:{df[col].dtype}")
    return df

def version_2_add_column(schema_dir,df_v1):
    print("\n" + "=" * 60)
    print("Version 2: Schema Evolution - Adding Columns")
    print("=" * 60)

    df_v2=df_v1.copy()

    df_v2['last_login']=pd.date_range(
        '2024-01-01',
        periods=len(df_v2),
        freq='h'
    )

    df_v2['subscription_level']='free'

    paid_indices=np.random.choice(len(df_v2),size=len(df_v2)//4,replace=False)

    df_v2.loc[paid_indices,'subscription_level']=np.random.choice(
        ['basic', 'premium', 'enterprise'],
        size=len(paid_indices)
    )

    print("New schema:")
    for col in df_v2.columns:
        print(f" - {col}: {df_v2[col].dtype}")

    print(f"Columns added: last_login, subscription_level")

    print("\n" + "-" * 60)
    print("Writing Version 2 file (with last_login and subscription_level)...")
    print("-" * 60)

    v2_dir=os.path.join(schema_dir,'version_2')
    os.makedirs(v2_dir,exist_ok=True)

    file_path=os.path.join(v2_dir,'version_2.parquet')
    df_v2.to_parquet(
        file_path,
        engine='pyarrow',
        compression='snappy'
    )

    print(f'\n Written {file_path}')
    return df_v2

def read_version1_columns_from_version2(schema_dir):
    v2_file=os.path.join(schema_dir,'version_2','version_2.parquet')
    
    v1_schema=pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_())
    ])

    print("-" * 60)
    print("Reading Version 2 File with Version 1 Schema as a Pandas DataFrame")
    print("-" * 60)

    df_v1_pandas = pd.read_parquet(v2_file, schema=v1_schema, engine='pyarrow')
    print(f"\nRead {len(df_v1_pandas)} rows from Version 2 file")
    print("Top 5 rows:")
    print(df_v1_pandas.head(5))

    return



def read_version2_columns_from_version1(schema_dir):
    v1_file = os.path.join(schema_dir, 'version_1', 'version_1.parquet')
    
    v2_schema=pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_()),
        pa.field('last_login', pa.date64()),           # Added in V2
        pa.field('subscription_level', pa.string())     # Added in V2
    ])

    print("\n" + "-" * 60)
    print("Reading Version 1 File with Version 2 Schema as PyArrow Table")
    print("-" * 60)

    table_v2_arrow = pq.read_table(v1_file, schema=v2_schema)
    print(f"\nRead {table_v2_arrow.num_rows} rows from Version 1 file")
    print("Top 5 rows:")
    print(table_v2_arrow.slice(0, 5).to_pandas())
    return
    

def demonstrate_schema_merging_strategy(schema_dir):
    
    print("\n" + "-" * 60)
    print("Schema merging strategy: merging V1 + V2 into V2 schema")
    print("-" * 60)

    v1_file = os.path.join(schema_dir, 'version_1', 'version_1.parquet')
    v2_file = os.path.join(schema_dir, 'version_2', 'version_2.parquet')

    v2_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_()),
        pa.field('last_login', pa.date64()),
        pa.field('subscription_level', pa.string())
    ])

    merged_df = pq.read_table(schema_dir, schema=v2_schema).to_pandas()

    if 'subscription_level' in merged_df.columns:
        merged_df['subscription_level'] = merged_df['subscription_level'].fillna('free')
    
    if 'last_login' in merged_df.columns:
        merged_df['last_login'] = pd.to_datetime(merged_df['last_login'])
        merged_df['last_login'] = merged_df['last_login'].fillna(pd.to_datetime('2000-01-01'))


    merged_dir = os.path.join(schema_dir, 'merged')
    os.makedirs(merged_dir, exist_ok=True)
    merged_path = os.path.join(merged_dir, 'merged_v2.parquet')

    merged_df.to_parquet(merged_path, engine='pyarrow', compression='snappy', index=False)

    print(f"\nWritten merged dataset to {merged_path}")

    merged_table = pq.read_table(merged_path, schema=v2_schema)

    print(f"\nRead {merged_table.num_rows} rows from merged file using V2 schema")
    print("Top 5 rows:")
    print(merged_table.slice(0, 5).to_pandas())

    return merged_table




def main():
    print("=" * 60)
    print("Parquet Recipe 4: Schema Evolution")
    print("=" * 60)

    schema_dir=setup_output_dir()

    df_v1=version_1_original_schema(schema_dir,num_records=1000)

    df_v2 = version_2_add_column(schema_dir, df_v1)

    read_version1_columns_from_version2(schema_dir)

    read_version2_columns_from_version1(schema_dir)

    demonstrate_schema_merging_strategy(schema_dir)


    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(" Schema evolution in Parquet is backward compatible")
    print(" Adding optional columns doesn't break old readers")
    print(" Mixed version files can be combined with proper null handling")
    print(" New columns are simply ignored by old code")
    print("=" * 60)


if __name__=='__main__':
    main()




