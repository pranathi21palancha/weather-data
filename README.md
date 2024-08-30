# Weather Data Integration Pipeline

This project simulates the integration of a third-party reporting tool (OpenWeatherMap API) with a data system. It includes an end-to-end ETL pipeline that extracts weather data, transforms it, and loads it into a PostgreSQL database.

## Project Overview

The Weather Data Integration Project demonstrates key data engineering skills by creating a pipeline that:

1. Extracts weather data from the OpenWeatherMap API
2. Transforms the raw data to fit a designed star schema
3. Loads the processed data into a PostgreSQL database
4. Orchestrates the entire ETL process using Apache Airflow

## Features

- API data extraction using the Requests library
- Data transformation using Pandas
- Data loading into PostgreSQL using SQLAlchemy
- ETL workflow orchestration with Apache Airflow
- Error handling and logging
- Unit testing for each component

## Tech Stack

- Python 3.8+
- PostgreSQL
- Apache Airflow
- SQLAlchemy
- Pandas
- Requests

## Project Structure

```
weather-data-integration/
│
├── dags/
│   └── weather_etl_dag.py
│
├── scripts/
│   ├── api_client.py
│   ├── data_transformer.py
│   ├── data_loader.py
│   └── sql_queries.py
│
├── tests/
│   ├── test_api_client.py
│   ├── test_data_transformer.py
│   └── test_data_loader.py
│
├── config/
│   └── config.ini
│
├── logs/
│
├── requirements.txt
├── README.md
└── .gitignore
```

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/bitsbard/weather-data-integration.git
   cd weather-data-integration
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   ```

3. Activate the virtual environment:
   - On Windows: `venv\Scripts\activate`
   - On macOS and Linux: `source venv/bin/activate`

4. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

5. Set up environment variables (see Configuration section)

## Configuration

Create a `.env` file in the project root with the following content:

```
OPENWEATHERMAP_API_KEY=your_api_key_here
POSTGRES_DB=weather_db
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

Replace the values with your actual API key and database credentials.

## Usage

1. Ensure your PostgreSQL database is running and accessible with the credentials provided in the `.env` file.

2. Initialize the Airflow database:
   ```
   airflow db init
   ```

3. Start the Airflow webserver:
   ```
   airflow webserver --port 8080
   ```

4. In a new terminal, start the Airflow scheduler:
   ```
   airflow scheduler
   ```

5. Access the Airflow web interface at `http://localhost:8080` and enable the `weather_etl_dag`.

The DAG will run daily, extracting weather data, transforming it, and loading it into your PostgreSQL database.

## Running Tests

To run the unit tests:

```
python -m unittest discover tests
```

## Data Model

The project uses a star schema with the following structure:

1. Fact Table: `weather_measurements`
   - date
   - city_id (foreign key to cities dimension)
   - temperature
   - humidity
   - pressure
   - wind_speed

2. Dimension Tables:
   - `cities`
     - city_id
     - city_name
     - country
     - latitude
     - longitude

## Technical Summary

This project demonstrates key data engineering concepts and tools. It uses Python for scripting, with libraries such as Requests for API interaction and Pandas for data manipulation. The ETL process is orchestrated using Apache Airflow, allowing for scheduled, repeatable workflows. Data is stored in a PostgreSQL database, interfaced with SQLAlchemy for ORM capabilities. The project structure follows best practices with separate modules for extraction (api_client.py), transformation (data_transformer.py), and loading (data_loader.py). Error handling, logging, and unit testing are implemented throughout to ensure robustness and reliability. Environment variables are used for configuration management, adhering to security best practices. This architecture demonstrates scalability and maintainability, key aspects of production-grade data engineering solutions.
