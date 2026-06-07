import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os


def generate_dirty_inventory_data(n_rows=100, scenario="mixed"):
    data=[]

    for i in range(n_rows):
        row = {
            'product_id': f'PROD_{i+1:03d}',
            'product_name': f'Product {i+1}',
            'price': round(random.uniform(10, 1000), 2),
            'stock_quantity': random.randint(0, 500),
            'category': random.choice(['Electronics', 'Clothing', 'Food', 'Books']),
            'last_updated': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'supplier_email': f'supplier{i+1}@example.com'
        }

        if scenario in ["mixed", "schema_errors"]:
            if i % 15 == 0: 
                row['product_id'] = f'INVALID_{i}'  
            if i % 20 == 0:  
                row['product_name'] = None  
            if i % 18 == 0:
                row['last_updated'] = 'invalid-date' 
        
        if scenario in ["mixed", "business_errors"]:
            if i % 12 == 0:  
                row['price'] = -round(random.uniform(10, 100), 2)  
            if i % 10 == 0:  
                row['stock_quantity'] = -random.randint(1, 50)  
            if i % 25 == 0:
                row['price'] = None  

        if scenario in ["mixed", "quality_errors"]:
            if i % 30 == 0:  
                row['supplier_email'] = 'invalid-email'  
            if i % 35 == 0:
                row['supplier_email'] = ''  
            if i % 8 == 0 and i > 0:  
                row = data[0].copy()  

        if scenario == "clean":
            pass  

        data.append(row)
    
    df=pd.DataFrame(data)
    if scenario in ["mixed", "schema_errors"]:
        df['unexpected_column'] = 'surprise!'
    
    return df

def save_datasets():
    os.makedirs('data/raw', exist_ok=True)
    df_dirty = generate_dirty_inventory_data(n_rows=100, scenario="mixed")
    df_dirty.to_csv('data/raw/inventory_dirty.csv', index=False)

if __name__=="__main__":
    save_datasets()

