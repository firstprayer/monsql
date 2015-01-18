'''
Postgres
--------
'''


import psycopg2
from db import Database
from table import Table
from config import TRANSACTION_MODE

class PostgreSQLTable(Table):
    
    def fetch_columns(self):
        sql = u"select column_name, data_type, character_maximum_length \
               from INFORMATION_SCHEMA.COLUMNS where table_name='%s';" %(self.name)
        self.cursor.execute(sql)
        columns = []
        for column in self.cursor.fetchall():
            column = column[0]
            columns.append(column)
        self.columns = columns


class PostgreSQLDatabase(Database):

    def __init__(self, host='127.0.0.1', port=5432, username='', password='',
                 dbname='test', mode=TRANSACTION_MODE.DEFAULT):
        db = psycopg2.connect("host=%s port=%d user=%s password=%s dbname=%s"
                              %(host, port, username, password, dbname))

        Database.__init__(self, db)

    def list_tables(self):
        self.cursor.execute('SELECT table_name \
                             FROM information_schema.tables \
                             ORDER BY table_name;')
        all_tablenames = [row[0].lower() for row in self.cursor.fetchall()]
        return all_tablenames

    def get_table_obj(self, name):
        table = PostgreSQLTable(db=self.db, name=name, mode=self.mode)
        return table

    def truncate_table(self, tablename):
        """
        Use 'TRUNCATE TABLE' to truncate the given table
        """
        self.cursor.execute('TRUNCATE TABLE %s' %tablename)
        self.db.commit()

        