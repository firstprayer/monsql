# coding=utf-8

from query import Query, QueryCondition, value_to_sql_str
from datetime import datetime, date
import types
from exception import MonSQLException
import uuid
from config import ASCENDING, DESCENDING

def from_none_to_null(v):
    if v is not None: return v
    return u'null'

def build_query(query):
    return QueryCondition(query).to_sql()

def build_select_query(table_name, values, query, sort=None, skip=0, limit=None):

    value_str = ""
    for index, field in enumerate(values):
        if field in ('index',):
            field = "'%s'" %(field)
            
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

    if limit is not None:
        sql = '%s LIMIT %d,%d' %(sql, skip, limit)

    # sprint sql
    return sql

# TODO: Allow user to give the subquery an alias
def build_select(query_obj):
    return build_select_query(query_obj.source, query_obj.fields, query_obj.filter, skip=query_obj.skip, \
                              limit=query_obj.limit, sort=query_obj.sort)

def build_insert(table_name, attributes):
    sql = "INSERT INTO %s" %(table_name)
    column_str = u""
    value_str = u""
    for index, (key, value) in enumerate(attributes.items()):
        if index > 0:
            column_str += u","
            value_str += u","
        column_str += key
        value_str += value_to_sql_str(value)
    sql = sql + u"(%s) VALUES(%s)" %(column_str, value_str)
    return sql

def build_delete(table_name, condition):
    query_str = build_query(condition)
    if not query_str:
        query_str = u""
    else:
        query_str = u"WHERE " + query_str
    sql = u"DELETE FROM %s %s" %(table_name, query_str)
    return sql

def build_update(table_name, condition, attributes):
    sql = u"UPDATE %s SET " %(table_name)    
    set_str = u""
    for index, (key, value) in enumerate(attributes.items()):
        if index > 0:
            set_str += u","
        set_str += key + u"=" + value_to_sql_str(value)

    query_str = build_query(condition)
    if query_str:
        query_str = u" WHERE " + query_str
    else:
        query_str = u""
    sql = sql + set_str + query_str
    return sql
