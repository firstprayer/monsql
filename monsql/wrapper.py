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
    This class should not be directly constructed. Should use MonSQL.get('name')
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
        """
        Commit the modification
        """
        self.db.commit()
        return self
    
    def count(self, distinct=False, distinct_fields=None):
        """
        :Parameters: 

        - distinct : boolean, whether use DISTINCT()
        - distinct_fields : list or tuple or strings. Each string is a column name used inside COUNT(). 
          If none, will use '*'

        :Return: The number of rows
        """
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
        :Examples:

        >>> users = user_table.find({'id': {'$in': [10, 20]}, 'age': {'$gt': 20}}) # Complex query
        >>> user_count = len(users)
        >>> for user in users:
        >>>     # Do something...
        >>>     print user.id
        >>> 
        >>> users = user_table.find({}, sort=[('age', monsql.ASCENDING)]) # sort by age

        Also support complex operators:

        >>> {a: 1}                                  # a == 1
        >>> {a: {$gt: 1}}                           # a > 1
        >>> {a: {$gte: 1}}                          # a >= 1
        >>> {a: {$lt: 1}}                           # a < 1
        >>> {a: {$lte: 1}}                          # a <= 1
        >>> {a: {$eq: 1}}                           # a == 1
        >>> {a: {$in: [1, 2]}}                      # a == 1
        >>> {a: {$contains: '123'}}                 # a like %123%
        >>> {$not: condition}                       # !(condition)
        >>> {$and: [condition1, condition2, ...]}   # condition1 and condition2
        >>> {$or: [condition1, condition2, ...]}    # condition1 or condition2

        :Parameters: 

        - query(dict): specify the WHERE clause. One example is {"name": "...", "id": ...}    
        - fields: specify what fields are needed
        - skip, limit: both integers, skip without defining limit is meaningless
        - sort: A list, each element is a two-item tuple, with the first item be the column name
          and the second item be either monsql.ASCENDING or monsql.DESCENDING

        :Return: a QuerySet object
        """
        if not fields:
            self.__ensure_columns()
            fields = self.columns

        query_obj = Query(source=self.name, filter=filter, fields=fields, skip=skip, limit=limit, sort=sort)
        return QuerySet(cursor=self.cursor, query=query_obj)

    
    def find_one(self, filter=None, fields=None, skip=0, sort=None):
        """
        Similar to find. This method only retrieve one. If no row matches, return None
        """
        result = self.find(filter=filter, fields=fields, skip=skip, limit=1, sort=sort)
        if len(result) > 0:
            return result[0]
        else:
            return None

    
    def insert(self, data_or_list_of_data):
        """
        :Examples:
        
        >>> user_table.insert({'username': 'Jude'}) # Insert one row
        >>> user_table.insert([{'username': 'Andy'}, {'username': 'Julia'}, ...]) # Insert multiple rows

        :Parameters:

        - data_or_list_of_data: Either a dict or a list of dict

        :Return: id or list of ids of inserted row
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

    
    def update(self, query, attributes, upsert=False):
        """
        :Parameters: 

        - query(dict), specify the WHERE clause
        - attributes(dict), specify the SET clause
        - upsert: boolean. If True, then when there's no row matches the query, insert the values

        :Return: Number of rows updated or inserted
        """
        if upsert:
            found_result = self.find_one(query)
            if not found_result:
                id = self.insert(attributes)
                if id > 0:
                    return 1
                else:
                    return 0

        sql = build_update(self.name, query, attributes)
        return self.cursor.execute(sql)


    
    def remove(self, filter=None):
        """
        :Parameters: 

        - query(dict), specify the WHERE clause

        :Return: Number of rows deleted
        """
        sql = build_delete(table_name=self.name, condition=filter)
        return self.cursor.execute(sql)


class MonSQL:
    """
    MongoDB style of using MySQL

    :Examples:

    >>> monsql = MonSQL(host, port, username, password, dbname)
    >>> user_table = monsql.get('user')
    >>> activated_users = user_table.find({'state': 2})
    >>> user_ids = user_table.insert([{'username': ...}, {'username': ...}, ...])
    >>> user_table.commit() # OR monsql.commit()

    """

    def __init__(self, host='127.0.0.1', port=3306, username='', password='', dbname='test', mode=TRANSACTION_MODE.DEFAULT):
        self.__db = MySQLdb.Connect(host=host, port=port, user=username, passwd=password, db=dbname)
        self.__cursor = self.__db.cursor()
        self.__table_map = {}
        self.__mode = mode

    def __ensure_table_obj(self, name):
        if not self.__table_map.has_key(name):
            self.__table_map[name] = self.__create_table_obj(name)

    def get(self, name):
        """
        Return a Table object to perform operations on this table. 

        Note that all tables returned by the samle MonSQL instance shared the same connection.

        :Parameters:

        - name: A table name

        :Returns: A Table object
        """
        self.__ensure_table_obj(name)
        return self.__table_map[name]

    def close(self):
        """
        Close the connection to the server
        """
        self.__db.close()
        self.__table_map = {}
        # self.__binded_map  = {}

    def commit(self):
        """
        Commit the current session
        """
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
        """
        Check whether the given table name exists in this database. Return boolean.
        """
        self.__cursor.execute('show tables')
        all_tablenames = [row[0].lower() for row in self.__cursor.fetchall()]
        tablename = tablename.lower()

        if tablename in all_tablenames:
            return True
        else:
            return False

    def create_table(self, tablename, columns, primary_key=None, force_recreate=False):
        """
        :Parameters:

        - tablename: string
        - columns: list or tuples, with each element be a string like 'id INT NOT NULL UNIQUE'
        - primary_key: list or tuples, with elements be the column names
        - force_recreate: When table of the same name already exists, if this is True, drop that table; if False, raise exception
        
        :Return: Nothing

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

    def drop_table(self, tablename, silent=False):
        """
        Drop a table

        :Parameters:

        - tablename: string
        - slient: boolean. If false and the table doesn't exists an exception will be raised;
          Otherwise it will be ignored
        
        :Return: Nothing

        """
        if not silent and not self.is_table_existed(tablename):
            raise MonSQLException('TABLE %s DOES NOT EXIST' %tablename)

        self.__cursor.execute('DROP TABLE IF EXISTS %s' %(tablename))
        self.__db.commit()

    def truncate_table(self, tablename):
        """
        Use 'TRUNCATE TABLE' to truncate the given table
        """
        self.__cursor.execute('TRUNCATE TABLE %s' %tablename)
        self.__db.commit()


