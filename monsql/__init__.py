# coding=utf-8
"""
MongoDB style of using Relational Database

:Examples:

>>> from monsql import MonSQL, DB_TYPES
>>> monsql = MonSQL(host, port, username, password, dbname, DB_TYPES.MYSQL)
>>> user_table = monsql.get('user')
>>> activated_users = user_table.find({'state': 2})
>>> user_ids = user_table.insert([{'username': ...}, {'username': ...}, ...])
>>> user_table.commit() # OR monsql.commit()

"""
from config import *
from wrapper_mysql import MySQLDatabase
from wrapper_sqlite3 import SQLite3Database
from wrapper_postgresql import PostgreSQLDatabase
from exception import MonSQLException

class DB_TYPES:
	MYSQL = 'MySQL'
	SQLITE3 = 'SQLite3'
	POSTGRESQL = 'PostgreSQL'

def MonSQL(host=None, port=None, username=None, password=None, dbname=None, dbpath=None, dbtype=None):
	"""
	Initialize and return a Database instance
	"""
	if dbtype is None:
		raise MonSQLException('Database type must be specified')

	if dbtype == DB_TYPES.MYSQL:
		return MySQLDatabase(host, port, username, password, dbname)
	elif dbtype == DB_TYPES.SQLITE3:
		return SQLite3Database(dbpath)
	elif dbtype == DB_TYPES.POSTGRESQL:
		return PostgreSQLDatabase(host, port, username, password, dbname)
	else:
		raise MonSQLException('Database type %s not supported' %dbtype)