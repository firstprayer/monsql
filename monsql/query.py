# coding=utf-8

import copy, types
from datetime import datetime, date
from exception import MonSQLException

"""
TODO: When using Query as source, not defining fields will lead to some problem, try to fix it
"""
class Query:
    def __init__(self, source=None, filter=None, fields=None, skip=0, limit=None, sort=None, alias=None):
        self.source = source
        self.filter = filter
        self.fields = fields
        self.sort = sort
        self.limit = limit
        self.skip  = skip
        self.alias = alias

    def clone(self):
        return Query(source=copy.deepcopy(self.source), 
                 filter=copy.deepcopy(self.filter), 
                 fields=copy.deepcopy(self.fields), 
                 skip=skip, 
                 limit=limit,
                 sort=copy.deepcopy(self.sort),
                 alias=copy.deepcopy(self.alias))   

    def add_filter(self, obj):
        self.filter = {'$and': [obj, self.filter]}

def value_to_sql_str(v):
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
        {$not: condition}                   -> !(condition)
        {$and: [condition1, condition2]}    -> condition1 and condition2
        {$or: [condition1, condition2]}     -> condition1 or condition2

        """
        condition = self.condition
        if condition is not None and len(condition.items()) > 0:
            keys = condition.keys()
            if len(keys) > 1:
                split_conditions = []
                for key in condition.keys():
                    split_conditions.append({key: condition[key]})

                return QueryCondition({'$and': split_conditions}).to_sql()
            else:
                query_field, query_value = condition.items()[0]

                if query_field in QueryCondition.COMPLEX_QUERY_INDICATOR:

                    if u'$not' == query_field:
                        not_condition = QueryCondition(query_value).to_sql()
                        if not_condition is not None:
                            return '!(%s)' %(not_condition)
                        else:
                            return None

                    if query_field in (u'$or', u'$and', ):
                        conditions = query_value
                        if not isinstance(conditions, list) or len(conditions) < 2:
                            raise MonSQLException('QUERY VALUE FOR KEY %s MUST BE LIST WITH LENGTH BEING AT LEAST 2' %(query_field))

                        conditions = map(lambda c: QueryCondition(c).to_sql(), conditions)
                        conditions = filter(lambda c: c is not None, conditions)

                        if len(conditions) > 0:
                            if query_field == u'$or':
                                return ' OR '.join(conditions)
                            elif query_field == u'$and':
                                return ' AND '.join(conditions)
                        else:
                            return None
                    else:
                        raise Exception('Unsupport query_field')
                else:
                    if query_field in QueryCondition.MYSQL_RESERVE_WORDS:
                        query_field = "'%s'" %(query_field)

                    if not type(query_value) is types.DictType:
                        query_value = {'$eq': query_value}

                    if len(query_value.keys()) > 1:
                        # Deal with situation like a: {'$gt': 1, '$lt': 10}
                        # Split into {$and: [a: {'$gt': 1}, a: {'$lt': 10}]}
                        split_conditions = []
                        for key in query_value.keys():
                            split_conditions.append(QueryCondition({query_field: {key: query_value[key]}}))

                        return QueryCondition({'$and': split_conditions}).to_sql()
                    else:
                        # The simple case of {a: {$eq: 1}}
                        match_key = query_value.keys()[0]
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
