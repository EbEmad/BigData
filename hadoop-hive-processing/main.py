import pandas as pd
import subprocess
import os
from datetime import datetime

def check_container_running(container_name):
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format={{.State.Running}}", container_name],
            capture_output=True, text=True, check=True
        )
        if result.stdout.strip() != "true":
            print(f"Error: Container {container_name} is not running.")
            return False
        return True
    except subprocess.CalledProcessError:
        print(f"Error: Container {container_name} does not exist.")
        return False

def copy_parquet_files_to_container():
    container_name = "namenode"
    if not check_container_running(container_name):
        return False
    container_path = f"{container_name}:/usr/local/"
    mimic_parquet_files = ["./data/ADMISSIONS.parquet"]

    for file in mimic_parquet_files:
        if not os.path.exists(file):
            print(f"Error: File {file} does not exist on the host.")
            return False
        try:
            subprocess.run(["docker", "cp", file, container_path], check=True)
            print(f"Copied {file} to {container_path}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error copying {file} to container: {str(e)}")
            return False

def preprocess_admissions():
    required_columns = ["HADM_ID", "SUBJECT_ID", "ADMITTIME", "DISCHTIME"]
    data_dir = "./data"
    input_file = os.path.join(data_dir, "ADMISSIONS.csv")
    output_parquet = os.path.join(data_dir, "ADMISSIONS.parquet")
    output_csv = os.path.join(data_dir, "ADMISSIONS_preprocessed.csv")

    os.makedirs(data_dir, exist_ok=True)

    try:
        if not os.path.exists(input_file):
            print(f"Error: {input_file} file not found.")
            return False
        admission_df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: {input_file} file not found.")
        return False

    if not all(col in admission_df.columns for col in required_columns):
        print(f"Error: Missing required columns in {input_file}. Required: {required_columns}")
        return False

    admission_df = admission_df.dropna(subset=required_columns)

    try:
        admission_df["HADM_ID"] = admission_df["HADM_ID"].astype(int)
        admission_df["SUBJECT_ID"] = admission_df["SUBJECT_ID"].astype(int)
        admission_df["ADMITTIME"] = pd.to_datetime(admission_df["ADMITTIME"], errors="coerce")
        admission_df["DISCHTIME"] = pd.to_datetime(admission_df["DISCHTIME"], errors="coerce")
        
        if admission_df["ADMITTIME"].isna().any() or admission_df["DISCHTIME"].isna().any():
            print("Warning: Some ADMITTIME or DISCHTIME values could not be converted to datetime.")
        
        admission_df["LOS"] = (admission_df["DISCHTIME"] - admission_df["ADMITTIME"]).dt.days
        admission_df.to_parquet(output_parquet, index=False)
        admission_df.to_csv(output_csv, index=False)
        print(f"Preprocessed {input_file} and saved as Parquet ({output_parquet}) and CSV ({output_csv}).")
        return True
    except Exception as e:
        print(f"Error during preprocessing: {str(e)}")
        return False

def upload_to_hdfs_from_container_home():
    container_name = "namenode"
    if not check_container_running(container_name):
        return False
    hdfs_path = "/data"
    mimic_csv_files = ["ADMISSIONS.parquet"]

    for file in mimic_csv_files:
        try:
            subprocess.run(
                ["docker", "exec", container_name, "hdfs", "dfs", "-put", f"/usr/local/{file}", hdfs_path],
                check=True
            )
            print(f"file: {file} from MIMIC-III parquet uploaded to HDFS.")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error uploading {file} to HDFS: {str(e)}")
            return False

def create_hive_tables():
    container_name = "hive-server"
    if not check_container_running(container_name):
        return False
    hive_commands = [
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS admissions (
            HADM_ID INT,
            SUBJECT_ID INT,
            ADMITTIME TIMESTAMP,
            DISCHTIME TIMESTAMP,
            LOS INT
        )
        STORED AS PARQUET
        LOCATION '/data';
        """
    ]

    for command in hive_commands:
        try:
            subprocess.run(
                ["docker", "exec", container_name, "hive", "-e", command],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Error creating Hive table: {str(e)}")
            return False
    print("Hive Tables created")
    return True

def run_hive_queries():
    container_name = "hive-server"
    if not check_container_running(container_name):
        return False
    hive_queries = [
        """
        SELECT SUBJECT_ID, COUNT(HADM_ID) AS ADMISSION_COUNT
        FROM admissions
        GROUP BY SUBJECT_ID
        ORDER BY ADMISSION_COUNT DESC
        LIMIT 10;
        """
    ]

    for query in hive_queries:
        try:
            result = subprocess.run(
                ["docker", "exec", container_name, "hive", "-e", query],
                capture_output=True, text=True, check=True
            )
            print(f"Hive query result:\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Error executing Hive query: {str(e)}")
            return False
    print("HiveQL queries executed.")
    return True

if __name__ == "__main__":
    # Step 1: Preprocess CSV and save as Parquet
    if not preprocess_admissions():
        print("Pipeline stopped due to preprocessing failure.")
        exit(1)

    # Step 2: Copy Parquet files to container
    if not copy_parquet_files_to_container():
        print("Pipeline stopped due to file copy failure.")
        exit(1)

    # Step 3: Upload Parquet files to HDFS
    if not upload_to_hdfs_from_container_home():
        print("Pipeline stopped due to HDFS upload failure.")
        exit(1)

    # Step 4: Create Hive tables
    if not create_hive_tables():
        print("Pipeline stopped due to Hive table creation failure.")
        exit(1)

    # Step 5: Run Hive queries
    if not run_hive_queries():
        print("Pipeline stopped due to Hive query execution failure.")
        exit(1)

    # Step 6: Pipeline completion
    print("Pipeline execution completed successfully.")