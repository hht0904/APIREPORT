from pyhive import hive
import pandas as pd

class Database:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    @property
    def connect(self):
        return hive.Connection(host=self.host, port=self.port)

    def execute_query(self, query):
        conn = self.connect
        df = pd.read_sql(query, conn)
        conn.close()
        return df

# Instantiate the Database object here
