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

    def __init__(self, host=None, port=None, username='', password='',
                 dbname='test', mode=TRANSACTION_MODE.DEFAULT):
        if port is None:
            port = 5432
            
        if host is None:
            host = '127.0.0.1'

        db = psycopg2.connect("host=%s port=%d user=%s password=%s dbname=%s"
                              %(host, port, username, password, dbname))

        Database.__init__(self, db)

    def list_tables(self):
        self.cursor.execute('SELECT table_schema, table_name \
                             FROM information_schema.tables \
                             ORDER BY table_name;')
        all_tablenames = map(lambda row: row[1].lower() if row[0] == 'public' \
                             else "%s.%s" % (row[0], row[1]), self.cursor.fetchall())
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

    def create_schema(self, schema_name):
        """
        Create schema. This method only implemented for this class
        """
        try:
            self.cursor.execute('CREATE SCHEMA %s' % schema_name)
        except Exception as e:
            raise e
        finally:
            self.db.commit()

    def drop_schema(self, schema_name, cascade=False):
        sql = 'DROP SCHEMA %s' %schema_name
        if cascade:
            sql += ' CASCADE'

        try:
            self.cursor.execute(sql)
        except Exception as e:
            raise e
        finally:
            self.db.commit()
        