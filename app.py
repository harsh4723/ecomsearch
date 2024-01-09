from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Connection parameters
db_params = {
    'host': 'postgres',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword'
}

@app.route('/')
def get_postgres_version():
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_params)

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Example: Execute a simple query
    cursor.execute("SELECT version();")
    version = cursor.fetchone()

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return jsonify({'PostgreSQL version': version[0]})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
