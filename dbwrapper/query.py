

class Query:
	def __init__(self, source=None, filter=None, fields=None, sort=None):
		self.source = source
		self.filter = filter
		self.fields = fields
		self.sort = sort

	