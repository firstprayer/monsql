# coding=utf-8

import copy, types
from datetime import datetime, date
from exception import MonSQLException


class Query:
    """
    Class contains all information need to do a 'SELECT' query
    """

    def __init__(self, source=None, filter=None, fields=None, skip=0, limit=None, sort=None, distinct=False, alias=None):
        self.source = source
        self.filter = filter
        self.fields = fields
        self.sort = sort
        self.limit = limit
        self.skip  = skip
        self.distinct = distinct
        self.alias = alias

    def clone(self):
        return Query(source=copy.deepcopy(self.source), 
                 filter=copy.deepcopy(self.filter), 
                 fields=copy.deepcopy(self.fields), 
                 skip=self.skip, 
                 limit=self.limit,
                 sort=copy.deepcopy(self.sort),
                 distinct=self.distinct,
                 alias=copy.deepcopy(self.alias))   

    def add_filter(self, obj):
        self.filter = {'$and': [obj, self.filter]}


def value_to_sql_str(v):
    """
    transform a python variable to the appropriate representation in SQL
    """
    if v is None:
        return 'null'

    if type(v) in (types.IntType, types.FloatType, types.LongType):
        return str(v)

    if type(v) in (types.StringType, types.UnicodeType):
        return "'%s'" %(v.replace(u"'", u"\\'"))

    if isinstance(v, datetime):
        return "'%s'" %(v.strftime("%Y-%m-%d %H:%M:%S"))

    if isinstance(v, date):
        return "'%s'" %(v.strftime("%Y-%m-%d"))

    return str(v)


class QueryCondition:

    MYSQL_RESERVE_WORDS                 = (u'index', )
    COMPLEX_QUERY_INDICATOR             = (u'$not', u'$and', u'$or')


    def __init__(self, condition):
        """
        condition: a dictionary, like {'id': 1} => id == 1
        """
        self.condition = condition

    def to_sql(self):
        """
        This function build a sql condition string (those used in the 'WHERE' clause) based on given condition
        Supported match pattern:

        {a: 1}                              -> a == 1
        {a: {$gt: 1}}                       -> a > 1
        {a: {$gte: 1}}                      -> a >= 1
        {a: {$lt: 1}}                       -> a < 1
        {a: {$lte: 1}}                      -> a <= 1
        {a: {$eq: 1}}                       -> a == 1
        {a: {$in: [1, 2]}}                  -> a == 1
        {a: {$contains: '123'}}             -> a like %123%

        And complex combination
        {$not: condition}                   -> NOT (condition)
        {$and: [condition1, condition2]}    -> condition1 and condition2
        {$or: [condition1, condition2]}     -> condition1 or condition2

        """
        condition = self.condition
        if condition:
            keys = condition.keys()

            if len(keys) > 1:
                # If in the form of {'a': 1, 'b': 2}, simplify to {'$and': [{'a': 1, 'b': 2}]}
                split_conditions = []
                for key in condition.keys():
                    split_conditions.append({key: condition[key]})

                return QueryCondition({'$and': split_conditions}).to_sql()

            else:
                query_field, query_value = condition.items()[0]

                if query_field in QueryCondition.COMPLEX_QUERY_INDICATOR:
                    # This is a composite query

                    if u'$not' == query_field:
                        not_condition = QueryCondition(query_value).to_sql()
                        if not_condition is not None:
                            return 'NOT (%s)' %(not_condition)
                        else:
                            return None

                    if query_field in (u'$or', u'$and', ):
                        conditions = query_value
                        if not isinstance(conditions, list) or len(conditions) < 2:
                            raise MonSQLException('QUERY VALUE FOR KEY %s MUST BE LIST WITH LENGTH BEING AT LEAST 2' %(query_field))

                        # compute sub conditions recursively
                        conditions = map(lambda c: QueryCondition(c).to_sql(), conditions)
                        conditions = filter(lambda c: c is not None, conditions)

                        # join them together
                        if len(conditions) > 0:
                            if query_field == u'$or':
                                return ' OR '.join(conditions)
                            elif query_field == u'$and':
                                return ' AND '.join(conditions)
                        else:
                            return None
                    else:
                        raise MonSQLException('Unsupport query_field')
                else:
                    # This is a one-field query like {'id': ...}

                    if query_field in QueryCondition.MYSQL_RESERVE_WORDS:
                        query_field = "`%s`" %(query_field)

                    if not type(query_value) is types.DictType:
                        # transform {'id': 1} to {'id': {'$eq': 1}} for convenience
                        query_value = {'$eq': query_value}

                    if len(query_value.keys()) > 1:
                        # Deal with situation like a: {'$gt': 1, '$lt': 10}
                        # Split into {$and: [a: {'$gt': 1}, a: {'$lt': 10}]}
                        split_conditions = []
                        for key in query_value.keys():
                            split_conditions.append(QueryCondition({query_field: {key: query_value[key]}}))

                        return QueryCondition({'$and': split_conditions}).to_sql()
                    else:
                        # The simple case of {a: {$complex_operator: 1}}
                        match_key = query_value.keys()[0] # the complex operator
                        match_value = query_value[match_key]

                        query_str = None
                        if u"$contains" == match_key:
                            query_str = u"LIKE " + value_to_sql_str('%' + match_value + '%')

                        elif match_key in ('$eq', '$gte', '$gt', '$lt', '$lte'):
                            map_dic = {'$eq': '=', '$gte': '>=', '$gt': '>', '$lt': '<', '$lte': '<='}
                            query_str = map_dic[match_key] + value_to_sql_str(match_value)

                        elif u'$in' == match_key:
                            if len(match_value) == 0:
                                query_str = u"IN (null) "
                            else:
                                query_str = u"IN (" + u','.join([str(_v_) for _v_ in match_value]) + u") "
                        else:
                            raise MonSQLException(u"Unsupport complex query: %s" %(match_key))

                        return query_field + ' ' + query_str
        else:
            return None

        # For testing
        assert False
