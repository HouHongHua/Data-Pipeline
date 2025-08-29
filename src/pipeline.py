import pandas as pd
import glob
import os
from dotenv import load_dotenv

def main():
    """Date ETL"""
    load_dotenv()

    raw_path = os.getenv("RAW_DATA_PATH", "data/raw")
    processed_path = os.getenv("PROCESSED_DATA_PATH", "data/processed")

    print("--- Start Data ETL ---")

    os.makedirs(processed_path, exist_ok=True)
    
    file_list = glob.glob(os.path.join(raw_path, 'green_tripdata_2025-*.parquet'))
    if not file_list:
        print(f"Error: '{raw_path}' do not find parquet files.")
        return

    print(f"Found {len(file_list)} files, preparing to merge...")
    df_list = [pd.read_parquet(file) for file in file_list]
    green_taxi = pd.concat(df_list, ignore_index=True)

    output_file = os.path.join(processed_path, 'green_tripdata_2025.parquet')
    green_taxi.to_parquet(output_file, engine='pyarrow')

    print(f"Data merging completed, saved to: {output_file}")
    print("--- Data ETL process finished ---")

if __name__ == '__main__':
    main()