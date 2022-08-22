from clickhouse_driver import connect
from clickhouse_driver.errors import Error
from sql.clickhouse_query import ClickhouseQuery
import os




class ClickhouseServer(object):
    def __init__(self):

        self.ch_host = os.environ["CH_HOST"]
        self.conn = None
        self.cursor = None
        self.query = ClickhouseQuery()

    def init(self):
        try:
            self.conn = connect("clickhouse://" + self.ch_host)
            self.cursor = self.conn.cursor()
        except Error as e:
            print(e.message)

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
        except Error as e:
            print(e.message)

    def insert_data(
        self,
        db_name,
        tb_name,
        columns,
        data,
    ):
        
        query = self.query.insert_data_to_table(db_name, tb_name, columns)
        print("inserting datas...")

        self.cursor.execute(query, data)

        print("successfully inserted")

    def create_table_with_columns(
        self,
        table,
        columns,
        db_name,
        order_by,
    ):
        query = self.query.create_table_with_columns(table, db_name, columns, order_by)

        self.execute_query(query)