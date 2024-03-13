import psycopg2
from env import *


class Connectors:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()
        self.cursor.close()

    def execute_sql(self, command):
        self.cursor.execute(command)
        self.connection.commit()
        return self.cursor.fetchall()
