import os
import sys
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the PYSPARK_SUBMIT_ARGS environment variable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/stuartmills/Documents/weather-data-integration/postgresql-42.7.4.jar pyspark-shell'

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

load_dotenv(os.path.join(project_root, '.env'))

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
DB_NAME = os.getenv('POSTGRES_DB')

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherETL") \
        .config("spark.jars", "/Users/stuartmills/Documents/weather-data-integration/postgresql-42.7.4.jar") \
        .config("spark.driver.extraClassPath", "/Users/stuartmills/Documents/weather-data-integration/postgresql-42.7.4.jar") \
        .getOrCreate()

def create_tables():
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    try:
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
        logger.info("Tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

def load_data(fact_df, dim_df):
    spark = create_spark_session()
    
    try:
        dim_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "cities") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .mode("overwrite") \
            .save()
        logger.info("Cities data loaded successfully")

        fact_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "weather_measurements") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .mode("append") \
            .save()
        logger.info("Weather measurements data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

    logger.info("Data loading process completed")

if __name__ == "__main__":
    create_tables()
    spark = create_spark_session()
    
    try:
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
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
    finally:
        spark.stop()
