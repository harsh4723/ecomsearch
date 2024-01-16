import psycopg2
import datetime

db_params = {
    'host': '35.245.250.43',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword'
}


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
    print("postgres version", version[0])

get_postgres_version()