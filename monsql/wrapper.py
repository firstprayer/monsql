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
import chardet

import MySQLdb as mdb
import types
from logging import Logger
from datetime import *
from query import Query, F
from sql import build_select, build_update, build_delete, build_insert
from queryset import QuerySet
from exception import MySQLManagerException

class TRANSACTION_MODE:
    AUTO = "auto"
    MANUAL = "manual"
    DEFAULT = "manual"
  

"""
This is a mongodb style MySQL Wrapper
Usage:
manager = MySQLManager(db=a MySQLdb db instance, source=source, mode=transaction_mode)

select: result = manager.find({"id": 1})
insert: manager.insert({"name": "xxx"})
update: manager.update({"name": "xxx"}, {"name": "xxxx"})
delete: manager.remove({"id": 2})

Setting source: manager.source(new_source), a source could be a string(table name), or a query object

Notice, in the DEFAULT case, one need to manually call manager.commit() after insert/update/delete
"""

class MySQLManager:
    def __init__(self, db, source=None, mode=None):
        self.db = db
        self.cursor = db.cursor()
        self.columns = None
        if source:
            self.source = source
        else:
            self.source = None
        if mode:
            self.transaction_mode = mode
        else:
            self.transaction_mode = TRANSACTION_MODE.DEFAULT

        self.logger = Logger("std")

    def _log_(self, log_info):
        print log_info

    def _check_source(self):
        if not self.source:
            raise MySQLManagerException("Source is not defined")

    def _check_source_to_be_table_name(self):
        pass
        # if not self.source or type(self.source) is not types.StringType:
        #     raise MySQLManagerException("Source is illegel")

    def _ensure_columns(self):
        if self.columns:
            return True
        self._fetch_columns()

    def _fetch_columns(self):
        self._check_source_to_be_table_name()
        if type(self.source) is types.StringType:
            sql = u"SHOW COLUMNS FROM %s" %(self.source)
            self.cursor.execute(sql)
            columns = []
            for column in self.cursor.fetchall():
                column = column[0]
                columns.append(column)
            self.columns = columns
        else:
            raise MySQLManagerException(u"Columns Unknown for non-table-name source")

    def commit(self):
        # self._log_("Committing")
        self.db.commit()
        return self
    """
    Set the source of the query
    """
    def set_source(self, source):
        self.source = source
        self.columns = None
        return self

    """
    @param:query(dict), specify the WHERE clause
    {"name": "...", "id": ...}
    @param: fields, specify what fields are needed
    @return a QuerySet object
    """
    def find(self, filter=None, fields=None, skip=0, limit=0, sort=None):
        if not fields:
            self._ensure_columns()
            fields = self.columns
        query_obj = Query(source=self.source, filter=filter, fields=fields, sort=sort)
        return QuerySet(cursor=self.cursor, query=query_obj)

    """
    similar to find
    @param: query(dict)
    @param: values: specifying what attributes are needed
    @return: a dict contains all the attributes or None if the query fail
    """
    def find_one(self, filter=None, fields=None):
        if not fields:
            self._ensure_columns()
            fields = self.columns

        query_obj = Query(source=self.source, filter=filter, fields=fields)
        sql = build_select(query_obj)

        self.cursor.execute(sql)
        data = self.cursor.fetchone()
        if not data:
            return data
        assert(len(data) == len(fields))
        result = {}
        for i in range(len(data)):
            result[fields[i]] = data[i]
        return result

    """
    @param:attributes(dict), specify the values clause
    @return success or not (boolean)
    """
    def insert(self, data_or_list_of_data):
        self._check_source_to_be_table_name()
        result = None

        def insert_data(data):
            sql = build_insert(self.source, data)
            row_count = self.cursor.execute(sql)
            # print sql.decode(chardet.detect(sql))
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
        self._check_source_to_be_table_name()

        if upsert:
            found_result = self.find_one(query)
            if not found_result:
                return self.insert(attributes)

        sql = build_update(self.source, query, attributes)
        self.cursor.execute(sql)
        return True


    """
    @param: query(dict), specify the WHERE clause
    @return success or not(boolean)
    """
    def remove(self, filter=None):
        if isinstance(self.source, Query):
            raise MySQLManagerException('CANNOT REMOVE ROW FROM SUBQUERY')
        self._check_source_to_be_table_name()
        sql = build_delete(table_name=self.source, filter=filter)
        # print sql
        self.cursor.execute(sql)
        return True 


class MonSQL:
    def __init__(self, db):
        self.db = db
        self.manager_map = {}

    def __getattr__(self, name):
        if not self.manager_map.has_key(name):
            self.manager_map[name] = self._create_manager_(name)

        return self.manager_map[name]

    def commit(self):
        self.db.commit()

    def _create_manager_(self, name):
        manager = MySQLManager(db=self.db, source=name)
        return manager