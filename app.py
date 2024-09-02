from flask import Flask, jsonify, render_template
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')
DB_NAME = os.getenv('POSTGRES_DB')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/weather_data')
def get_weather_data():
    engine = create_engine(DATABASE_URL)
    query = """
    SELECT c.city_name, w.date, w.temperature, w.humidity
    FROM weather_measurements w
    JOIN cities c ON w.city_id = c.city_id
    ORDER BY w.date DESC
    LIMIT 100
    """
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
