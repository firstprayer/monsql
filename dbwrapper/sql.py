from query import *
from datetime import *
import types
from exception import MySQLManagerException
import uuid
"""
TODO: Support more types
"""
def tosqlstr(v):
    if type(v) is types.IntType or type(v) is types.FloatType or type(v) is types.LongType:
        return str(v)
    if type(v) is types.StringType:
        return "'%s'" %(v)
    if isinstance(v, datetime):
        return "'%s'" %(v.strftime("%Y-%m-%d %H:%M:%S"))
    if isinstance(v, date):
        return "'%s'" %(v.strftime("%Y-%m-%d"))
    if isinstance(v, F):
        return v.field_name
    return str(v)


def build_query(query):
    query_str = ""
    if query and len(query.items()) > 0:
        for index, (key, value) in enumerate(query.items()):
            if index > 0:
                query_str += " and "
            if type(value) is types.DictType:
                if "$contains" in value.keys():
                    query_str += key + " LIKE %" + str(value) + "%"
                else:
                    raise MySQLManagerException("Unsupport complex query")
            query_str += key + "=" + tosqlstr(value)
        return query_str
    else:
        return None

def build_select_query(table_name, values, query, sort=None):
    value_str = ""
    for index, field in enumerate(values):
        if index == 0:
            value_str = field
        else:
            value_str += "," + field

    # Where clause
    query_str = build_query(query)
    if query_str:
        query_str = "WHERE " + query_str
    else:
        query_str = ""
    sql = """SELECT %s FROM %s %s""" %(value_str, table_name, query_str)
    return sql

# TODO: Allow user to give the subquery an alias
def build_select(query_obj):
    source = query_obj.source
    if isinstance(source, Query):
        if not hasattr(source, "alias"):
            raise MySQLManagerException("Using subquery as source requires an alias!")
            return
        source = "(%s) as %s" %(build_select(source), source.alias)
    return build_select_query(source, query_obj.fields, query_obj.filter)

def build_insert(table_name, attributes):
    sql = "INSERT INTO %s" %(table_name)
    column_str = ""
    value_str = ""
    for index, (key, value) in enumerate(attributes.items()):
        if index > 0:
            column_str += ","
            value_str += ","
        column_str += key
        value_str += tosqlstr(value)
    sql = sql + "(%s) VALUES(%s)" %(column_str, value_str)
    return sql

def build_delete(table_name, filter):
    query_str = build_query(filter)
    if not query_str:
        query_str = ""
    else:
        query_str = "WHERE " + query_str
    sql = "DELETE FROM %s %s" %(table_name, query_str)
    return sql

def build_update(table_name, filter, attributes):
    sql = "UPDATE %s SET " %(table_name)    
    set_str = ""
    for index, (key, value) in enumerate(attributes.items()):
        if index > 0:
            set_str += ","
        set_str += key + "=" + tosqlstr(value)

    query_str = build_query(filter)
    if query_str:
        query_str = " WHERE " + query_str
    else:
        query_str = ""
    sql = sql + set_str + query_str
    return sql