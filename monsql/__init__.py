# coding=utf-8

from config import *
from wrapper_mysql import MySQL
from wrapper_sqlite3 import SQLite3
from exception import MonSQLException

class DB_TYPES:
	MYSQL = 'MySQL'
	SQLITE3 = 'SQLite3'

def MonSQL(host=None, port=None, username=None, password=None, dbname=None, dbpath=None, dbtype=None):
	if dbtype is None:
		raise MonSQLException('Database type must be specified')

	if dbtype == DB_TYPES.MYSQL:
		return MySQL(host, port, username, password, dbname)
	elif dbtype == DB_TYPES.SQLITE3:
		return SQLite3(dbpath)
	else:
		raise MonSQLException('Database type %s not supported' %dbtype)