import csv
import sys

def check_name(row):
    if not row.get("name") or row.get("name").lower().strip()!= f"c{row['id']}":
        return False
    return True

def process_data():

    
    reader = csv.DictReader(sys.stdin)
    
    fieldnames = ["id", "name", "sal"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    
    writer.writeheader()
    
    for row in reader:
        
        if not check_name(row):
            row["name"] = f"c{row['id']}"
            
        writer.writerow(row)

if __name__ == "__main__":
    process_data()