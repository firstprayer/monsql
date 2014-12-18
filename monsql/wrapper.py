#encoding=utf-8

"""
This is a module for using mysql as simple as using mongodb
Support mongodb manipulation like:

* find(queryObj, fieldObject)
* find_one(queryObj, fieldObject)
* insert(keyValueObj)
* insert_batch(a list of keyValueObj)
* update(queryObj, keyValueObj)
* remove(queryObj)

The design for this module would take **Pymongo** as a reference

Since it's a module for mysql, transaction must be considered, so we also provide:
* commit(): commit the transaction

We plan to Support Table join
We would also support subquery
"""
from config import ASCENDING, DESCENDING
import MySQLdb
import types
from logging import Logger
from datetime import *
from query import Query
from sql import build_select, build_update, build_delete, build_insert
from queryset import QuerySet
from exception import MonSQLException

class TRANSACTION_MODE:
    AUTO = "auto"
    MANUAL = "manual"
    DEFAULT = "manual"
  

class Table:
    """
    Usage:
    select: result = manager.find({"id": 1})
    insert: manager.insert({"name": "xxx"})
    update: manager.update({"name": "xxx"}, {"name": "xxxx"})
    delete: manager.remove({"id": 2})
    """
    def __init__(self, db, name, mode=None):
        self.db = db
        self.cursor = db.cursor()
        self.columns = None
        self.name = name

        if mode:
            self.transaction_mode = mode
        else:
            self.transaction_mode = TRANSACTION_MODE.DEFAULT

        self.logger = Logger("std")

    def _log_(self, log_info):
        print log_info

    def __ensure_columns(self):
        if self.columns:
            return True
        self.__fetch_columns()

    def __fetch_columns(self):
        sql = u"SHOW COLUMNS FROM %s" %(self.name)
        self.cursor.execute(sql)
        columns = []
        for column in self.cursor.fetchall():
            column = column[0]
            columns.append(column)
        self.columns = columns

    def commit(self):
        self.db.commit()
        return self
    
    def count(self, distinct=False, distinct_fields=None):
        if distinct_fields is None:
            field = '*'
        else:
            field = ','.join(distinct_fields)

        if distinct:
            count_str = 'DISTINCT(%s)' %(field)
        else:
            count_str = field
        count_str = 'COUNT(%s)' %(count_str)

        sql = 'SELECT %s FROM %s' %(count_str, self.name)
        self.cursor.execute(sql)
        count = self.cursor.fetchone()[0]

        return count

    
    
    def find(self, filter={}, fields=None, skip=0, limit=None, sort=None):
        """
        @param:query(dict), specify the WHERE clause
        {"name": "...", "id": ...}
        @param: fields, specify what fields are needed
        skip, limit: both integers, skip without defining limit is meaningless
        @return a QuerySet object
        """
        if not fields:
            self.__ensure_columns()
            fields = self.columns

        query_obj = Query(source=self.name, filter=filter, fields=fields, skip=skip, limit=limit, sort=sort)
        return QuerySet(cursor=self.cursor, query=query_obj)

    
    def find_one(self, filter=None, fields=None, skip=0, sort=None):
        """
        similar to find
        @param: query(dict)
        @param: values: specifying what attributes are needed
        @return: a RowData contains all the attributes or None if the query fail
        """
        result = self.find(filter=filter, fields=fields, skip=skip, limit=1, sort=sort)
        if len(result) > 0:
            return result[0]
        else:
            return None

    
    def insert(self, data_or_list_of_data):
        """
        @param:attributes(dict), specify the values clause
        @return: id or list of ids of inserted row
        """
        result = None

        def insert_data(data):
            sql = build_insert(self.name, data)
            row_count = self.cursor.execute(sql)

            if row_count:
                return self.cursor.lastrowid
            else:
                return None

        value_to_be_inserted = []

        if isinstance(data_or_list_of_data, list):
            result = []
            for data in data_or_list_of_data:
                r = insert_data(data)
                result.append(r)
        else:
            result = insert_data(data_or_list_of_data)
        
        return result

    """
    @param: query(dict), specify the WHERE clause
    @param: attributes(dict), specify the SET clause
    @return success or not (boolean)
    """
    def update(self, query, attributes, upsert=False):

        if upsert:
            found_result = self.find_one(query)
            if not found_result:
                return self.insert(attributes)

        sql = build_update(self.name, query, attributes)
        return self.cursor.execute(sql)


    """
    @param: query(dict), specify the WHERE clause
    @return success or not(boolean)
    """
    def remove(self, filter=None):
        sql = build_delete(table_name=self.name, condition=filter)
        return self.cursor.execute(sql)


class MonSQL:
    """
    Usage:
    monsql = MonSQL(host, port, username, password, dbname)
    Notice, in the DEFAULT case, one need to manually call monsql.commit() after insert/update/delete
    """

    def __init__(self, host='127.0.0.1', port=3306, username='', password='', dbname='test', mode=TRANSACTION_MODE.DEFAULT):
        self.__db = MySQLdb.Connect(host=host, port=port, user=username, passwd=password, db=dbname)
        self.__cursor = self.__db.cursor()
        self.__table_map = {}
        # self.__binded_map  = {}
        self.__mode = mode

    def __ensure_table_obj(self, name):
        if not self.__table_map.has_key(name):
            self.__table_map[name] = self.__create_table_obj(name)

    # def bind(self, name):
    #     """
    #     Bind a table name so that one can get the manager object simply by using monsql.TABLE_NAME instead
    #     of using monsql.get(TABLE_NAME)
    #     """
    #     if hasattr(self, name):
    #         raise MonSQLException('TABLE NAME CONFLICTS, PLEASE USE .get(...)')
    #     self.__binded_map[name] = self.get(name)

    def get(self, name):
        self.__ensure_table_obj(name)
        return self.__table_map[name]

    def close(self):
        self.__db.close()
        self.__table_map = {}
        # self.__binded_map  = {}

    def commit(self):
        self.__db.commit()

    def __create_table_obj(self, name):
        table = Table(db=self.__db, name=name, mode=self.__mode)
        return table

    def set_foreign_key_check(self, to_check):
        if to_check:
            self.__db.cursor().execute('SET foreign_key_checks = 1;')
        else:
            self.__db.cursor().execute('SET foreign_key_checks = 0;')

    def is_table_existed(self, tablename):
        self.__cursor.execute('show tables')
        all_tablenames = [row[0].lower() for row in self.__cursor.fetchall()]
        tablename = tablename.lower()

        if tablename in all_tablenames:
            return True
        else:
            return False

    def create_table(self, tablename, columns, primary_key=None, force_recreate=False):
        """
        tablename: string
        columns: list or tuples, with each element be a string like 'id INT NOT NULL UNIQUE'
        primary_key: list or tuples, with elements be the column names
        force_recreate: When table of the same name already exists, if this is True, drop that table; if False, raise exception
        """
        if self.is_table_existed(tablename):
            if force_recreate:
                self.drop_table(tablename)
            else:
                raise MonSQLException('TABLE ALREADY EXISTS')

        columns_specs = ','.join(columns)
        if primary_key is not None:
            if len(primary_key) == 0:
                raise MonSQLException('PRIMARY KEY MUST AT LEAST CONTAINS ONE COLUMN')

            columns_specs += ',PRIMARY KEY(%s)' %(','.join(primary_key))

        sql = 'CREATE TABLE %s(%s)' %(tablename, columns_specs)
        self.__cursor.execute(sql)
        self.__db.commit()

    def drop_table(self, tablename, slient=False):
        """
        Drop a table
        tablename: string
        slient: boolean. When the table doesn't exists, this be False will raise an exception; otherwise no action will be taken
        """
        if not self.is_table_existed(tablename) and not slient:
            raise MonSQLException('TABLE %s DOES NOT EXIST' %tablename)

        self.__cursor.execute('DROP TABLE IF EXISTS %s' %(tablename))
        self.__db.commit()

    def truncate_table(self, tablename):
        self.__cursor.execute('TRUNCATE TABLE %s' %tablename)
        self.__db.commit()


