from sqlite3 import OperationalError
from pyflightdata import FlightData
import psycopg2

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )

        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def insertToDatabase(logs: list, connection):
    logs = [
        ( "SU1248", "scheduled" "20221130"),
    ]

    log_records = ", ".join(["%s"] * len(logs))

    insert_query = (
        f"INSERT INTO log (planeNumber, status, date) VALUES {log_records}"
    )
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(insert_query, logs)