# coding=utf-8

from query import Query
from sql import build_select
from exception import MonSQLException

class DataRow:

    def __init__(self, keyvalue_map):
        self.data = keyvalue_map

    def __getattr__(self, attrname):
        if self.data.has_key(attrname):
            return self.data[attrname]
        else:
            raise AttributeError('%s does not exist' %attrname)

    def __setattr(self, attrname, value):
        self.data[attrname] = value



"""
Lazy load data
"""
class QuerySet:

    def __init__(self, cursor, query):
        self.cursor = cursor
        self.query = query
        self._data = None
        self._need_to_refetch_data = False

    """
    Python magic functions
    """
    def __len__(self):
        if not self._data or self._need_to_refetch_data:
            self._fetch_data()
        return len(self._data)

    def __iter__(self):
        if not self._data or self._need_to_refetch_data:
            self._fetch_data()
        return iter(self._data)

    def __getitem__(self, k):
        if not self._data or self._need_to_refetch_data:
            self._fetch_data()
        return self._data[k]

    def _fetch_data(self):

        sql = build_select(self.query)
        # print sql
        self.cursor.execute(sql)
        # Wrap the data into dictionaries
        data_list = self.cursor.fetchall()
        values = self.query.fields
        result_list = []
        for data in data_list:
            assert(len(data) == len(values))
            result = {}
            for i in range(len(data)):
                result[values[i]] = data[i]
            result_list.append(DataRow(result))

        self._data = result_list
        self._need_to_refetch_data = False

    def filter(self, filter):
        new_query_set = self.clone()
        new_query_set.query.set_filter_fields(filter)
        
        return new_query_set

    def clone(self):
        return QuerySet(cursor=self.cursor, query=self.query.clone())

    def exists(self):
        return len(self) > 0

    def sort(self, sort):
        self._need_to_refetch_data = True
        raise Exception('NOT IMPLEMENTED')

    def values(self):
        return [v for v in self]

