import json
import os
from OpenSSL import SSL
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, render_template, request
from datetime import datetime
from flask_cors import CORS

load_dotenv()

# Load environment variables
# FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY') or 'dev' ***Is this needed for anything?****
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

app = Flask(__name__)
CORS(app)
# app.config['SECRET_KEY'] = FLASK_SECRET_KEY

# Connect to the database
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return conn

@app.route('/submissions')
def submissions():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS count FROM test")
    count = cur.fetchall()
    cur.close()
    conn.close()
    count[0]['count'] = int(count[0]['count'])
    return json.dumps(count[0])

@app.route('/submissions_time')
def submissions_time():
    pass

@app.route('/escs')
def escs():
    pass

@app.route('/learning_hpw')
def learning_time():
    conn = get_db_connection()
    cur = conn.cursor()
    datasets = {'datasets' : None}
    cur.execute('''
    WITH int_test AS(
	SELECT cnt, cast(tmins AS int)
	FROM test
	WHERE tmins != 'NA')
    SELECT cnt AS country, AVG(tmins)/60 AS hours 
    FROM int_test
    GROUP BY cnt
                ''')
    hpw = cur.fetchall()
    datasets["datasets"] = hpw
    cur.close()
    conn.close()
   
    return datasets



@app.route('/early_education_and_belonging')
def early_education_and_belonging():
    pass

if __name__ == '__main__':
    context = ("cert.pem", "key.pem")
    app.run(debug=True, port = 5000, host='0.0.0.0', ssl_context=context)