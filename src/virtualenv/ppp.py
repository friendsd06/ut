import psycopg2
from psycopg2 import sql

# Database connection details
db_config = {
    "host": "localhost",
    "port": "5432",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# Function to fetch data from the demo table
def fetch_data():
    try:
        # Establish a connection to the database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # SQL query to select all data from the demo table
        query = sql.SQL("SELECT id, name, description, created_at, is_active FROM demo")
        cursor.execute(query)

        # Fetch all rows from the result
        rows = cursor.fetchall()

        # Print the fetched data
        for row in rows:
            print(f"ID: {row[0]}, Name: {row[1]}, Description: {row[2]}, Created At: {row[3]}, Is Active: {row[4]}")

    except psycopg2.Error as e:
        print(f"Error reading data from PostgreSQL table: {e}")

    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Call the function to fetch and print data
fetch_data()
pip install psycopg2
python script_name.py