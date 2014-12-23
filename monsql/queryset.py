# coding=utf-8

from query import Query
from sql import build_select
from exception import MonSQLException

class DataRow:

    def __init__(self, keyvalue_map):
        self.__data = keyvalue_map

    def __getattr__(self, attrname):

        if self.data.has_key(attrname):
            return self.data[attrname]
        else:
            raise AttributeError('%s does not exist' %attrname)

    @property
    def data(self):
        return self.__data





class QuerySet:
    """
    Lazy load data
    """

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
        """
        Add new filter to the query set. Note that since QuerySet is lazy, it would
        just combine this filter with the original filter with an 'AND'
        :Parameters: 
        - filter: a dictionary
        :Return: a new QuerySet object
        """
        new_query_set = self.clone()
        new_query_set.query.add_filter(filter)
        
        return new_query_set

    def limit(self, n, skip=None):
        """
        Limit the result set. However when the query set already has limit field before,
        this would raise an exception
        :Parameters:
        - n : The maximum number of rows returned
        - skip: how many rows to skip
        :Return: a new QuerySet object so we can chain operations
        """
        if self.query.limit is not None:
            raise MonSQLException('LIMIT already defined')

        new_query_set = self.clone()
        new_query_set.query.limit = n
        new_query_set.query.skip = skip

        return new_query_set

    def clone(self):
        return QuerySet(cursor=self.cursor, query=self.query.clone())

    def exists(self):
        return len(self) > 0

    def sort(self, sort):
        self._need_to_refetch_data = True
        raise Exception('NOT IMPLEMENTED')

    def values(self):
        return [v.data for v in self]

