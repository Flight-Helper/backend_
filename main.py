import pika, sys, os
from datetime import datetime
from sqlite3 import OperationalError
from pyflightdata import FlightData
import psycopg2
import DatabaseOperations
from config import *


class DataNearest:

    def __init__(self, numberOfFlight : str, status : str, date : str ):
        self.numberOfFlight = numberOfFlight
        self.status = status
        self.date = date



def main():
    connectionDB = DatabaseOperations.create_connection(
        NAME_DATABASE, PGUSER, PGPASSWORD, DB_IP, "5432"
    )
    create_table = """
    CREATE TABLE IF NOT EXISTS log (
      id SERIAL PRIMARY KEY,
      plane_number VARCHAR(15) NOT NULL,
      status VARCHAR(30) NOT NULL,
      date VARCHAR(100)
    )
    """
    DatabaseOperations.execute_query(connectionDB, create_table)

    f = FlightData()
    connectionMQ = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connectionMQ.channel()

    channel.queue_declare(queue='flights')

    def callback(ch, method, properties, body):
        data = []
        if method.routing_key == 'get-nearest-flights':
            numberOfFlight = body.decode('utf-8')
            info = f.get_history_by_flight_number(numberOfFlight, limit=20)
            for i in info:
                timestamp = datetime.now().timestamp()
                res = i['time']['estimated']['departure']
                if res == "None":
                    res = i['time']['scheduled']['departure_millis']
                d1 = DataNearest(numberOfFlight, i['status']['text'], res)
                data.append(d1)
                res = int(res)
                if res > timestamp * 1000:
                    print(datetime.fromtimestamp(res / 1000))
        if method.routing_key == 'get-flight-status':
            flight = body.decode('utf-8')
            info = f.get_flight_for_date(flight.numberOfFlight, flight.date)
            status = info[0]['status']['text']
            data.append(status)
            logs = [
                (flight.numberOfFlight, status, flight.date),
            ]
            DatabaseOperations.insertToDatabase(logs, connectionDB)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='flights')
        channel.basic_publish(exchange='', routing_key='flights', body=data)
        connection.close()
        print(" [x] Received %r" % body.decode('UTF-8'))


    channel.basic_consume(queue='flights', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages')
    channel.start_consuming()

if __name__ == '__main__':
    main()


