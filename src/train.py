import pandas as pd
import numpy as np
import os
import joblib

from dotenv import load_dotenv
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

## train model
def train_tip_prediction_model(df: pd.DataFrame, training_months: list, testing_months: list):
    if df.empty:
        print("Empty Data")
        return

    TARGET = 'tip_amount'

    Features = [
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "extra",
        "mta_tax",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "trip_type",
        "cbd_congestion_fee",
        "pickup_hour", 
        "trip_duration_minutes",
    ]

    df['pickup_hour'] = df['lpep_pickup_datetime'].dt.hour
    df['trip_duration_minutes'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 60
    df['pickup_month_str'] = df['lpep_pickup_datetime'].dt.strftime('%m')

    train_df = df[df['pickup_month_str'].isin(training_months)].copy()
    test_df = df[df['pickup_month_str'].isin(testing_months)].copy()

    train_df = train_df[[*Features, TARGET]].dropna()
    test_df = test_df[[*Features, TARGET]].dropna()

    X_train = train_df[Features]
    y_train = train_df[TARGET]
    X_test = test_df[Features]
    y_test = test_df[TARGET]

    categorical_features = ['RatecodeID', 'pickup_hour', 'trip_type']
    numerical_features = [col for col in train_df.columns if col not in categorical_features and col not in TARGET]

    numerical_transformer = StandardScaler()
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_features),
            ('cat', categorical_transformer, categorical_features)
        ])
    
    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', LinearRegression())
    ])

    model_pipeline.fit(X_train, y_train)
    print("Train Finished.")

    y_pred = model_pipeline.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    print(f"Mean Squared Error (MSE): {mse:.2f}")
    print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
    print(f"R-squared (R2): {r2:.2f}")
    
    model_output_path = 'model/green_taxi_tip_prediction_model.joblib'
    joblib.dump(model_pipeline, model_output_path)
    print(f"\n Save Model: {model_output_path}")


def main():
    load_dotenv()
    processed_path = os.getenv("PROCESSED_DATA_PATH", "data/processed")

    print("--- Start Model Training ---")

    processed_file = os.path.join(processed_path, 'green_tripdata_2025.parquet')
    if not os.path.exists(processed_file):
        print(f"Error: do not find parquet files. '{processed_file}' First run pipeline.py")
        return

    green_taxi = pd.read_parquet(processed_file)
    
    TRAINING_MONTHS = ['01', '02', '03', '04']
    TESTING_MONTHS = ['05', '06']

    train_tip_prediction_model(green_taxi, TRAINING_MONTHS, TESTING_MONTHS)
    print("--- End Model Training ---")

if __name__ == '__main__':
    main()