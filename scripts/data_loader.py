import os
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, Date, Time, MetaData
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
DB_NAME = os.getenv('POSTGRES_DB')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_tables():
    engine = create_engine(DATABASE_URL)
    metadata = MetaData()

    cities = Table('cities', metadata,
        Column('city_id', Integer, primary_key=True),
        Column('city_name', String),
        Column('country', String),
        Column('latitude', Float),
        Column('longitude', Float)
    )

    weather_measurements = Table('weather_measurements', metadata,
        Column('id', Integer, primary_key=True),
        Column('date', Date),
        Column('time', Time),
        Column('city_id', Integer),
        Column('temperature', Float),
        Column('humidity', Integer),
        Column('pressure', Integer),
        Column('wind_speed', Float)
    )

    metadata.create_all(engine)
    return engine, cities, weather_measurements

def load_data(fact_df, dim_df):
    engine, cities, weather_measurements = create_tables()

    with engine.connect() as connection:
        for _, row in dim_df.iterrows():
            insert_stmt = insert(cities).values(row.to_dict())
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['city_id'],
                set_={c.key: c for c in insert_stmt.excluded if c.key != 'city_id'}
            )
            connection.execute(on_conflict_stmt)

        for _, row in fact_df.iterrows():
            connection.execute(weather_measurements.insert().values(row.to_dict()))

    print("Data loaded successfully.")

if __name__ == "__main__":
    import pandas as pd

    # Test data
    fact_data = {
        'date': ['2023-05-01', '2023-05-01'],
        'time': ['12:00:00', '12:00:00'],
        'city_id': [0, 1],
        'temperature': [22.5, 15.3],
        'humidity': [60, 72],
        'pressure': [1015, 1008],
        'wind_speed': [18.36, 15.12]
    }

    dim_data = {
        'city_id': [0, 1],
        'city_name': ['New York', 'London'],
        'country': ['US', 'GB'],
        'latitude': [40.7128, 51.5074],
        'longitude': [-74.0060, -0.1278]
    }

    fact_df = pd.DataFrame(fact_data)
    dim_df = pd.DataFrame(dim_data)

    load_data(fact_df, dim_df)
