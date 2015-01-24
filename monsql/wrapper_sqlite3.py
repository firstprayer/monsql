'''
SQLite3
-------
A lightweight disk-based database. Usually useful for internal
data storage and prototyping for applications.
'''


import sqlite3
from db import Database
from table import Table

class SQLite3Table(Table):
    def fetch_columns(self):
        sql = u"PRAGMA table_info(%s)" %(self.name)
        self.cursor.execute(sql)
        columns = []

        for column in self.cursor.fetchall():
            column = column[1]
            columns.append(column)
        self.columns = columns


class SQLite3Database(Database):

    def __init__(self, file_path=None):
        if file_path is None: file_path = ":memory:"
        db = sqlite3.connect(file_path)
        Database.__init__(self, db)

    def list_tables(self):
        self.cursor.execute('''SELECT name FROM sqlite_master WHERE type = 'table' ''')
        all_tablenames = [row[0].lower() for row in self.cursor.fetchall()]
        return all_tablenames

    def get_table_obj(self, name):
        table = SQLite3Table(db=self.db, name=name, mode=self.mode)
        return table

    def truncate_table(self, tablename):
        """
        SQLite3 doesn't support direct truncate, so we just use delete here
        """
        self.get(tablename).remove()
        self.db.commit()

    
        