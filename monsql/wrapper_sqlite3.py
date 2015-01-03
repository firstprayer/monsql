'''
SQLite3
-------
A lightweight disk-based database. Usually useful for internal
data storage and prototyping for applications.
'''


import sqlite3
from db import Database


class SQLite3(Database):
    def __init__(self, file_path=None):
        if file_path is None: file_path = ":memory:"
        self.__db = sqlite3.connect(file_path)
        Database.__init__(self, self.__db)
