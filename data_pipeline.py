import os
import requests
import argparse
import psycopg2
from psycopg2.extras import execute_values
import duckdb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
db_params = {
    "dbname": os.getenv("DB"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PW"),
    "host": os.getenv("HOST"),
    "port": os.getenv("PORT")
}

def download_data(url):
    local_filename = url.split('/')[-1]
    # Check if the file already exists
    if not os.path.exists(local_filename):
        print(f"File '{local_filename}' is downloading.")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                # Set the chunk size to 256 KB
                for chunk in r.iter_content(chunk_size=262144):  
                    f.write(chunk)
        print(f"File '{local_filename}' has downloaded.")
    else:
        print(f"File '{local_filename}' already exists. Skipping download.")
    return local_filename


def standardize_column_names(df):
    standardized_columns = {
        'VendorID': 'vendor_id',
        'tpep_pickup_datetime': 'tpep_pickup_datetime',
        'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
        'passenger_count': 'passenger_count',
        'trip_distance': 'trip_distance',
        'RatecodeID': 'ratecode_id',
        'store_and_fwd_flag': 'store_and_fwd_flag',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id',
        'payment_type': 'payment_type',
        'fare_amount': 'fare_amount',
        'extra': 'extra',
        'mta_tax': 'mta_tax',
        'tip_amount': 'tip_amount',
        'tolls_amount': 'tolls_amount',
        'improvement_surcharge': 'improvement_surcharge',
        'total_amount': 'total_amount',
        'congestion_surcharge': 'congestion_surcharge',
        'Airport_fee': 'airport_fee'
    }
    df.rename(columns=standardized_columns, inplace=True)
    return df

def process_data_with_duckdb(data_file):
    conn = duckdb.connect()
    print(f"Processing file {data_file}")
    df = conn.execute(f"SELECT * FROM parquet_scan('{data_file}')").df()
    df = standardize_column_names(df)
    print(f"{data_file} processing complete")
    conn.close()
    return df

def create_table_if_not_exists(conn, table_name, schema_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                vendor_id INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count FLOAT,
                trip_distance FLOAT,
                ratecode_id FLOAT,
                store_and_fwd_flag VARCHAR,
                pu_location_id INT,
                do_location_id INT,
                payment_type INT,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT,
                airport_fee FLOAT
            );
        """)
        conn.commit()

def insert_data(conn, df, table_name, schema_name):

    # Prepare data for batch insert
    tuples = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(list(df.columns))
    query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES %s"

    # Execute batch insert
    with conn.cursor() as cur:
        execute_values(cur, query, tuples)
    conn.commit()

def create_tracking_table_if_not_exists(conn, tracking_table_name, schema_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{tracking_table_name} (
                file_name VARCHAR PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def is_file_processed(conn, file_name, tracking_table_name, schema_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 1 FROM {schema_name}.{tracking_table_name}
            WHERE file_name = %s;
        """, (file_name,))
        return cur.fetchone() is not None

def mark_file_as_processed(conn, file_name, tracking_table_name, schema_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema_name}.{tracking_table_name} (file_name)
            VALUES (%s)
            ON CONFLICT (file_name) DO NOTHING;
        """, (file_name,))
        conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Data pipeline")
    parser.add_argument(
        "--input_file", 
        type=str, 
        help="Path to the input file to be processed.",
        default="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet"
        )
    parser.add_argument(
        "--table",
        type=str,
        default="raw_yellow_taxi",
        help="Postgresql table to load processed data"
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="data_pipelines",
        help="Schema name in Postgresql database to create tables"
    )
    args = parser.parse_args()

    try:
        # Connect to PostgreSQL
        with psycopg2.connect(**db_params) as conn:
            # Create tracking table
            create_tracking_table_if_not_exists(conn, "processed_files", args.schema)
           
            # Download data
            data_file = download_data(args.input_file)
            
            # Check if file is already processed
            if is_file_processed(conn, data_file, "processed_files", args.schema):
                print(f"File '{data_file}' has already been processed. Skipping.")
                return

            # Process the data with DuckDB
            df = process_data_with_duckdb(data_file)

            create_table_if_not_exists(conn, args.table, args.schema)
            print("Loading data into postgres")
            insert_data(conn, df, args.table, args.schema)
            
            # Mark file as processed
            mark_file_as_processed(conn, data_file, "processed_files", args.schema)

        print("Data loaded successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
