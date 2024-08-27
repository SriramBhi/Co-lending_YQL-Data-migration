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
postgres_db = ''
postgres_user = ''
postgres_password = ''
postgres_host = ''
postgres_port = ''
table_name = 's'

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

def process_chunk(chunk_query, file_index):
    parquet_file = f'temp_{file_index}.parquet'
    
    # Connect to DuckDB and run the query to fetch the chunk
    con = duckdb.connect(database=':memory:')
    con.sql(chunk_query).write_parquet(parquet_file)

    # Upload to S3
    s3_path = f"{s3_folder}{parquet_file}"
    upload_to_s3(parquet_file, s3_path)

def export_postgres_to_parquet():
    con = duckdb.connect(database=':memory:')
    
    # Connect to PostgreSQL
    con.execute(f"""
    SET postgres_enable_http_proxy=1;
    INSTALL postgres_scanner;
    LOAD postgres_scanner;
    """)
    
    # Scan the PostgreSQL table using the DuckDB connection
    con.sql(f"""
    COPY (
        SELECT * FROM '{postgres_db}.{table_name}' 
        WHERE INDEX_CONDITION -- Adjust this based on your indexing
    ) TO 's3://{s3_bucket_name}/{s3_folder}file.parquet' 
    (FORMAT PARQUET, PARTITION BY chunk_size)
    """)
    
    # Get the number of rows and the estimated number of chunks based on file size limit
    total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    rows_per_chunk = int(parquet_file_size_limit / con.execute(f"SELECT avg_row_size FROM {table_name}").fetchone()[0])
    total_chunks = total_rows // rows_per_chunk + (1 if total_rows % rows_per_chunk else 0)
    
    threads = []
    for i in range(total_chunks):
        offset = i * rows_per_chunk
        chunk_query = f"SELECT * FROM {table_name} LIMIT {rows_per_chunk} OFFSET {offset}"
        thread = threading.Thread(target=process_chunk, args=(chunk_query, i))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("Data export complete.")

if __name__ == "__main__":
    export_postgres_to_parquet()
