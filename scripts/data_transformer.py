import pandas as pd
from datetime import datetime

def transform_weather_data(raw_data):
    df = pd.DataFrame(raw_data)
    
    # Convert datetime string to datetime object
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    # Extract date and time into separate columns
    df['date'] = df['datetime'].dt.date
    df['time'] = df['datetime'].dt.time
    
    # Round temperature to 1 decimal place
    df['temperature'] = df['temperature'].round(1)
    
    # Convert wind speed from m/s to km/h
    df['wind_speed'] = (df['wind_speed'] * 3.6).round(1)
    
    # Create a unique identifier for each city
    df['city_id'] = df.groupby(['city_name', 'country']).ngroup()
    
    # Split into fact and dimension dataframes
    fact_columns = ['date', 'time', 'city_id', 'temperature', 'humidity', 'pressure', 'wind_speed']
    fact_df = df[fact_columns]
    
    dim_columns = ['city_id', 'city_name', 'country', 'latitude', 'longitude']
    dim_df = df[dim_columns].drop_duplicates()
    
    return fact_df, dim_df

def get_latest_batch_id():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

if __name__ == "__main__":
    # Test data
    test_data = [
        {
            'city_name': 'New York',
            'country': 'US',
            'latitude': 40.7128,
            'longitude': -74.0060,
            'temperature': 22.5,
            'humidity': 60,
            'pressure': 1015,
            'wind_speed': 5.1,
            'datetime': '2023-05-01T12:00:00'
        },
        {
            'city_name': 'London',
            'country': 'GB',
            'latitude': 51.5074,
            'longitude': -0.1278,
            'temperature': 15.3,
            'humidity': 72,
            'pressure': 1008,
            'wind_speed': 4.2,
            'datetime': '2023-05-01T12:00:00'
        }
    ]
    
    fact_df, dim_df = transform_weather_data(test_data)
    print("Fact DataFrame:")
    print(fact_df)
    print("\nDimension DataFrame:")
    print(dim_df)
    
    print(f"\nLatest Batch ID: {get_latest_batch_id()}")
