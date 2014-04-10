# coding=utf-8

from query import *
from datetime import *
import types
from exception import MySQLManagerException
import uuid
from config import ASCENDING, DESCENDING
"""
TODO: Support more types
"""
def tosqlstr(v):
    if v is None:
        return 'null'
    if type(v) is types.IntType or type(v) is types.FloatType or type(v) is types.LongType:
        return str(v)
    if type(v) is types.StringType or type(v) is types.UnicodeType:
        return "'%s'" %(v.replace(u"'", u"\\'"))
    if isinstance(v, datetime):
        return "'%s'" %(v.strftime("%Y-%m-%d %H:%M:%S"))
    if isinstance(v, date):
        return "'%s'" %(v.strftime("%Y-%m-%d"))
    if isinstance(v, F):
        return v.field_name
    return str(v)

def from_none_to_null(v):
    if v is not None: return v
    return u'null'

def build_query(query):
    query_str = ""
    if query and len(query.items()) > 0:
        for index, (key, value) in enumerate(query.items()):
            if index > 0:
                query_str += u" and "
            if type(value) is types.DictType:
                for complex_key in value.keys():
                    if u"$contains" == complex_key:
                        query_str += key + u" LIKE '%" + str(value[complex_key]) + u"%' "
                    elif u'$in' == complex_key:
                        if len(value[complex_key]) == 0:
                            query_str += key + u" IN (null) "
                        else:
                            query_str += key + u" IN (" + u','.join([str(_v_) for _v_ in value[complex_key]]) + u") "
                    elif u'$notIn' == complex_key:
                        if len(value[complex_key]) == 0:
                            query_str += key + u" NOT IN (null) "
                        else:
                            query_str += key + u" NOT IN (" + u','.join([str(_v_) for _v_ in value[complex_key]]) + u") "
                    elif complex_key == u'$gte':#in [_i_[0] for _i_ in [('$gte', ' >= ')]]:
                        query_str += key + ' >= ' + tosqlstr(value[complex_key])
                    else:
                        raise MySQLManagerException(u"Unsupport complex query")
            else:
                query_str += key + u"=" + tosqlstr(from_none_to_null(value))

        return query_str
    else:
        return None

def build_select_query(table_name, values, query, sort=None):
    value_str = ""
    for index, field in enumerate(values):
        if index == 0:
            value_str = field
        else:
            value_str += u"," + field

    # Where clause
    query_str = build_query(query)
    if query_str:
        query_str = u"WHERE " + query_str
    else:
        query_str = ""
    sql = u"""SELECT %s FROM %s %s""" %(value_str, table_name, query_str)
    if sort:
        sort_strings = []

        for item in sort:
            assert len(item) == 2
            assert item[1] in (ASCENDING, DESCENDING)
            sort_str = '%s ' %(item[0])
            if item[1] == ASCENDING:
                sort_str += 'ASC'
            elif item[1] == DESCENDING:
                sort_str += 'DESC'
            sort_strings.append(sort_str)

        sql = '''%s ORDER BY %s''' %(sql, ','.join(sort_strings))
    # sprint sql
    return sql

# TODO: Allow user to give the subquery an alias
def build_select(query_obj):
    source = query_obj.source
    if isinstance(source, Query):
        if not hasattr(source, u"alias"):
            raise MySQLManagerException(u"Using subquery as source requires an alias!")
            return
        source = u"(%s) as %s" %(build_select(source), source.alias)
    return build_select_query(source, query_obj.fields, query_obj.filter, query_obj.sort)

def build_insert(table_name, attributes):
    sql = "INSERT INTO %s" %(table_name)
    column_str = u""
    value_str = u""
    for index, (key, value) in enumerate(attributes.items()):
        if index > 0:
            column_str += u","
            value_str += u","
        column_str += key
        value_str += tosqlstr(value)
    sql = sql + u"(%s) VALUES(%s)" %(column_str, value_str)
    return sql

def build_delete(table_name, filter):
    query_str = build_query(filter)
    if not query_str:
        query_str = u""
    else:
        query_str = u"WHERE " + query_str
    sql = u"DELETE FROM %s %s" %(table_name, query_str)
    return sql

def build_update(table_name, filter, attributes):
    sql = u"UPDATE %s SET " %(table_name)    
    set_str = u""
    for index, (key, value) in enumerate(attributes.items()):
        if index > 0:
            set_str += u","
        set_str += key + u"=" + tosqlstr(value)

    query_str = build_query(filter)
    if query_str:
        query_str = u" WHERE " + query_str
    else:
        query_str = u""
    sql = sql + set_str + query_str
    return sql
