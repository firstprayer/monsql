# coding=utf-8

import copy

"""
TODO: When using Query as source, not defining fields will lead to some problem, try to fix it
"""
class Query:
	def __init__(self, source=None, filter=None, fields=None, sort=None, alias=None):
		self.source = source
		self.filter = filter
		self.fields = fields
		self.sort = sort
		self.alias = alias
	def clone(self):
		return Query(source=copy.deepcopy(self.source), 
			     filter=copy.deepcopy(self.filter), 
			     fields=copy.deepcopy(self.fields), 
			     sort=copy.deepcopy(self.sort), 
			     alias=copy.deepcopy(self.alias))	

	def set_filter_fields(self, obj):
		for key, value in obj.items():
			self.filter[key] = value

class F:
	def __init__(self, field_name):
		self.field_name = field_name
