import duckdb
import boto3
import threading
import os

# AWS S3 Configuration
s3_bucket_name = ''
s3_folder = ''
aws_access_key = ''
aws_secret_key = ''
aws_region = ''

# PostgreSQL Configuration
postgres_db = 'metadb'
postgres_user = 'postgres'
postgres_password = ''
postgres_host = 'localhost'
postgres_port = '5432'
table_name = 'event_metadata'

# Parquet file size limit (in bytes)
parquet_file_size_limit = 500 * 1024 * 1024  # 500MB

# S3 Client
s3_client = boto3.client('s3', 
                         region_name=aws_region,
                         aws_access_key_id=aws_access_key,
                         aws_secret_access_key=aws_secret_key)

def upload_to_s3(file_path, s3_path):
    s3_client.upload_file(file_path, s3_bucket_name, s3_path)
    os.remove(file_path)  # Remove the file after uploading to S3
    print(f"Uploaded {file_path} to {s3_path}")

def process_chunk(con,parquet_file): #Should add an indexing parameter to seggregate data.

    con.sql(f"""COPY (SELECT * FROM events.{table_name}) TO '{parquet_file}' (FORMAT PARQUET)""")

    # Upload to S3
    s3_path = f"{s3_folder}{parquet_file}"
    upload_to_s3(parquet_file, s3_path)

def export_postgres_to_parquet():
    threads = []
    con = duckdb.connect(database=':memory:')

    # Connect to PostgreSQL
    con.execute(f"""
    INSTALL postgres;
    LOAD postgres;
    """)

    con.execute(f"""
                ATTACH 'dbname=metadb user=postgres host=127.0.0.1' AS events (TYPE POSTGRES);
                """)
    
    # Should loop this according to the indexing parameter and parquet file size.
    parquet_file = f'/Users/sriramreddy.bhimavarapu/Desktop/Data-migration/temp_parquet.parquet'
    thread = threading.Thread(target=process_chunk, args=(con,parquet_file))
    threads.append(thread)
    thread.start()
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("Data export complete.")

if __name__ == "__main__":
    export_postgres_to_parquet()
