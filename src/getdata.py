import os
import glob
import argparse
import pandas as pd
import logging
from config import get_db_engine, RAW_DATA_PATH, PROCESSED_DATA_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_monthly_data(engine, target_months: list, data_name_pattern: str):

    table_name = 'green_tripdata'
    file_pattern = os.path.join(RAW_DATA_PATH, data_name_pattern)
    all_files = sorted(glob.glob(file_pattern))

    logging.info(f"{RAW_DATA_PATH} Find {data_name_pattern} Data")

    for file_path in all_files:
        filename = os.path.basename(file_path)
        try:
            month_str = filename.split('-')[1].split('.')[0]
            if month_str in target_months:
                logging.info(f"Processing: {filename}...")
                df = pd.read_parquet(file_path)
                
                df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000)

                logging.info(f"Successfully imported {filename} into '{table_name}' table.")
        except IndexError:
            logging.warning(f"Filename format is incorrect, skipping: {filename}")
        except Exception as e:
            logging.error(f"Error occurred while processing file {filename}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Taxi Data Ingestion into PostgreSQL Database")
    parser.add_argument('--months', nargs='+', required=True, help="List of months to import")
    args = parser.parse_args()

    engine = get_db_engine()
    try:
        data_name_pattern = 'green_tripdata_2025-*.parquet'
        ingest_monthly_data(engine, args.months, data_name_pattern)
        logging.info("All specified months' data ingestion completed.")
    finally:
        if engine:
            engine.dispose()
            logging.info("Close the Database Connection")

if __name__ == '__main__':
    main()