import json
import os
from OpenSSL import SSL
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, jsonify
from datetime import datetime
from flask_cors import CORS

load_dotenv()

# Load environment variables
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
    cur.execute("SELECT COUNT(*) AS count FROM live")
    count = cur.fetchall()
    cur.close()
    conn.close()
    count[0]['count'] = int(count[0]['count'])
    return json.dumps(count[0])

@app.route('/submissions_time')
def submissions_time():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT hour, subs_per_hour FROM time')
    sub_time = cur.fetchall()
    datasets = {
        "datasets": [
            {
                "id": "Submissions",
                "data": [{"x": f"{entry['hour']:02}:00", "y": entry['subs_per_hour']} for entry in sub_time]
            }
        ]
    }
    cur.close()
    conn.close()
    return jsonify(datasets)

@app.route('/escs')
def escs():
    conn = get_db_connection()
    cur = conn.cursor()
    datasets = {'datasets' : None}
    cur.execute('''
    WITH int_test AS(
	SELECT cnt, cast(escs AS float)
	FROM live
	WHERE escs != 'NA')
    SELECT cnt AS id, AVG(escs) AS value 
    FROM int_test
    GROUP BY id
    ORDER BY id
                ''')
    escs = cur.fetchall()
    datasets["datasets"] = escs
    cur.close()
    conn.close()
    return datasets

@app.route('/learning_hpw')
def learning_time():
    conn = get_db_connection()
    cur = conn.cursor()
    datasets = {'datasets' : None}
    cur.execute('''
    WITH int_test AS(
	SELECT cnt, cast(tmins AS int)
	FROM live
	WHERE tmins != 'NA')
    SELECT cnt AS country, AVG(tmins)/60 AS hours 
    FROM int_test
    GROUP BY cnt
    ORDER BY cnt
                ''')
    hpw = cur.fetchall()
    datasets["datasets"] = hpw
    cur.close()
    conn.close()
    return datasets

@app.route('/early_education_and_belonging')
def early_education_and_belonging():
    conn = get_db_connection()
    cur = conn.cursor()
    datasets = {'datasets' : None}
    cur.execute('''
    WITH int_test AS(
	SELECT cnt,cast(durecec AS integer), cast(belong AS float)
	FROM live
	WHERE durecec != 'NA' AND belong != 'NA')
    SELECT cnt AS id, ROUND(AVG(durecec),0) AS x, AVG(belong) AS y, COUNT(cnt) AS submissions
    FROM int_test
    GROUP BY cnt
    ORDER BY cnt
                ''')
    results = cur.fetchall()
    formatted_data = []
    for result in results:
        country_data = {
            "id": result["id"],
            "data": [
                {
                    "x": int(float(result["x"])),  # Convert x to an integer
                    "y": float(result["y"]),      # Keep y as a float
                    "submissions": int(result["submissions"])  # Convert submissions to an integer
                }
            ]
        }
        formatted_data.append(country_data)
    final_output = {"datasets": formatted_data}
    cur.close()
    conn.close()
    return final_output

if __name__ == '__main__':
    context = ("cert.pem", "key.pem")
    app.run(debug=True, port = 5000, host='0.0.0.0', ssl_context=context)