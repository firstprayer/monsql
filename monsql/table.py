# coding=utf-8
import abc
from logging import Logger
from config import TRANSACTION_MODE
from query import Query
from queryset import QuerySet
from sql import build_query, build_select, build_update, build_delete, build_insert
  

class Table:
    """
    A collection of related data entries in columns and rows.
    This class should not be directly constructed. Instead use
    Database.get('table_name') to have a table returned.
    """

    db = None
    cursor = None
    columns = None
    name = None

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


    def columns():
        doc = "The columns property."
        def fget(self):
            return self._columns
        def fset(self, value):
            self._columns = value
        def fdel(self):
            del self._columns
        return locals()

    columns = property(**columns())


    @abc.abstractmethod
    def fetch_columns(self):
        pass


    def __ensure_columns(self):
        if self.columns is not None:
            return True
        self.fetch_columns()
        return True


    def commit(self):
        """
        Ends current transaction, making permanent any changes made.
        """
        self.db.commit()
        return self
    

    def count(self, query=None, distinct=False, distinct_fields=None):
        """
        Returns the number of rows satisying a criteria, if provided.
        
        :Parameters: 

        - query: specify the WHERE clause
        - distinct : boolean, whether to use DISTINCT()
        - distinct_fields : list or tuple or strings. Each string is
          a column name used inside COUNT(). If none, '*' will be
          used.

        :Return: int, the number of rows
        """
        if distinct_fields is None:
            self.__ensure_columns()
            if distinct:
                field = ','.join(self.columns)
            else:
                field = '*'
        else:
            field = ','.join(distinct_fields)

        if distinct:
            count_str = 'DISTINCT(%s)' %(field)
        else:
            count_str = field

        count_str = 'COUNT(%s)' %(count_str)

        sql = 'SELECT %s FROM %s' %(count_str, self.name)

        if query is not None:
            query_str = build_query(query)
            if query_str:
                sql = sql + ' WHERE ' + query_str

        self.cursor.execute(sql)
        count = self.cursor.fetchone()[0]

        return count


    def find(self, filter={}, fields=None, skip=0, limit=None, sort=None):
        """
        Searches the table using the filters provided.
        
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
        >>> {a: {$in: [1, 2]}}                      # a == 1 or a == 2
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
        Similar to find. This method will only retrieve one row.
        If no row matches, returns None
        """
        result = self.find(filter=filter, fields=fields, skip=skip, limit=1, sort=sort)
        if len(result) > 0:
            return result[0]
        else:
            return None


    def insert(self, data_or_list_of_data):
        """
        Insert data into the table.
        
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
        Updates data in the table.
        
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
        Removes rows from the table.
        
        :Parameters: 

        - query(dict), specify the WHERE clause

        :Return: Number of rows deleted
        """
        sql = build_delete(table_name=self.name, condition=filter)
        return self.cursor.execute(sql)

