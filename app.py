import json
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, render_template, request

load_dotenv()

# Load environment variables
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY') or 'dev'
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# If any database environment variables is not set, raise an error
if DB_HOST is None:
    raise ValueError('DB_HOST is not set')
elif DB_NAME is None:
    raise ValueError('DB_NAME is not set')
elif DB_USERNAME is None:
    raise ValueError('DB_USERNAME is not set')
elif DB_PASSWORD is None:
    raise ValueError('DB_PASSWORD is not set')

app = Flask(__name__,)
app.config['SECRET_KEY'] = FLASK_SECRET_KEY

# Connect to the database
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    if conn:
        print("It works")
    else:
        print("It doesn't work")
    return conn

@app.route('/submissions')
def submissions():
    get_db_connection()
    

if __name__ == '__main__':
    app.run(debug=True)