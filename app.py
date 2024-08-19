from flask import Flask, render_template
import snowflake.connector
import os


app = Flask(__name__)

# Snowflake connection
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user="aaayli",
        password="Gudik400",
        account="SF31807",
        warehouse="COMPUTE_WH",
        database="SYSTEM_SERVICES",
        schema="ELASTICSEARCH",
    )
    return conn

# Retrieve logs by log level
def get_logs():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    query = """
        SELECT LOGLEVEL, TASKID, MESSAGE, EXECUTIONTIME 
        FROM APPLICATION_LOGS
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    logs = {
        'INFO': [],
        'WARNING': [],
        'ERROR': []
    }

    for row in rows:
        log = {
            'TASKID': row[1],
            'MESSAGE': row[2],
            'EXECUTIONTIME': row[3]
        }
        logs[row[0]].append(log)
    
    return logs

@app.route('/')
def index():
    logs = get_logs()
    return render_template('index.html', logs=logs)

if __name__ == '__main__':
    app.run(debug=True)
