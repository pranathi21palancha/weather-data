import os
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
DB_NAME = os.getenv('POSTGRES_DB')

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherETL") \
        .config("spark.jars", "/absolute/path/to/postgresql-42.2.23.jar") \
        .getOrCreate()

def create_tables():
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    with engine.connect() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS cities (
                city_id INT PRIMARY KEY,
                city_name VARCHAR(255),
                country VARCHAR(2),
                latitude FLOAT,
                longitude FLOAT
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS weather_measurements (
                id SERIAL PRIMARY KEY,
                date DATE,
                time TIME,
                city_id INT,
                temperature FLOAT,
                humidity INT,
                pressure INT,
                wind_speed FLOAT
            )
        """))
    print("Tables created successfully.")

def load_data(fact_df, dim_df):
    spark = create_spark_session()
    
    dim_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "cities") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .mode("overwrite") \
        .save()

    fact_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "weather_measurements") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .mode("append") \
        .save()

    print("Data loaded successfully.")

if __name__ == "__main__":
    create_tables()
    spark = create_spark_session()
    
    fact_data = [
        ('2023-05-01', '12:00:00', 0, 22.5, 60, 1015, 18.36),
        ('2023-05-01', '12:00:00', 1, 15.3, 72, 1008, 15.12)
    ]
    fact_df = spark.createDataFrame(fact_data, ["date", "time", "city_id", "temperature", "humidity", "pressure", "wind_speed"])

    dim_data = [
        (0, 'New York', 'US', 40.7128, -74.0060),
        (1, 'London', 'GB', 51.5074, -0.1278)
    ]
    dim_df = spark.createDataFrame(dim_data, ["city_id", "city_name", "country", "latitude", "longitude"])

    load_data(fact_df, dim_df)
