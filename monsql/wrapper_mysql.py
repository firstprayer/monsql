'''
MySQL
--------
The World's "most" popular open source database
'''


import MySQLdb
from db import Database
from table import Table
from config import TRANSACTION_MODE

class MySQLTable(Table):
    
    def fetch_columns(self):
        sql = u"SHOW COLUMNS FROM %s" %(self.name)
        self.cursor.execute(sql)
        columns = []
        for column in self.cursor.fetchall():
            column = column[0]
            columns.append(column)
        self.columns = columns


class MySQL(Database):

    def __init__(self, host='127.0.0.1', port=3306, username='', password='',
                 dbname='test', mode=TRANSACTION_MODE.DEFAULT):
        db = MySQLdb.Connect(host=host, port=port, user=username,
                                    passwd=password, db=dbname)
        Database.__init__(self, db)

    def list_tables(self):
        self.cursor.execute('show tables')
        all_tablenames = [row[0].lower() for row in self.cursor.fetchall()]
        return all_tablenames

    def get_table_obj(self, name):
        table = MySQLTable(db=self.db, name=name, mode=self.mode)
        return table

    def truncate_table(self, tablename):
        """
        Use 'TRUNCATE TABLE' to truncate the given table
        """
        self.cursor.execute('TRUNCATE TABLE %s' %tablename)
        self.db.commit()

        