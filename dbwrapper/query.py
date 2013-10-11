

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

class F:
	def __init__(self, field_name):
		self.field_name = field_name