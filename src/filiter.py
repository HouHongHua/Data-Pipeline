import os
import pandas as pd
import logging
from config import get_db_engine, PROCESSED_DATA_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_and_export_data(engine):
    """
    從資料庫查詢、篩選資料，並匯出為 Parquet 檔案。
    """
    query = """
    SELECT
        "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID", "fare_amount",
        "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee",
        "improvement_surcharge", "total_amount", "congestion_surcharge",
        "cbd_congestion_fee"
    FROM green_tripdata
    WHERE "payment_type" = 1
      AND "fare_amount" > 0
      AND "tip_amount" >= 0
      AND "trip_distance" > 0
      AND "total_amount" > 0;
    """

    logging.info("正在從資料庫執行查詢...")
    try:
        df = pd.read_sql(query, engine)
        logging.info(f"查詢完成，共獲取 {len(df)} 筆資料。")

        os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
        output_path = os.path.join(PROCESSED_DATA_PATH, 'green_tripdata_filtered.parquet')
        
        df.to_parquet(output_path, index=False)
        logging.info(f"已將篩選後的資料儲存至: {output_path}")
        print("\n資料預覽:")
        print(df.head())

    except Exception as e:
        logging.error(f"執行查詢或儲存檔案時發生錯誤: {e}")

def main():
    engine = get_db_engine()
    try:
        transform_and_export_data(engine)
    finally:
        if engine:
            engine.dispose()
            logging.info("資料庫連線已關閉。")

if __name__ == '__main__':
    main()