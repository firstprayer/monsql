import unittest
import MySQLdb
import sys
from datetime import *
from wrapper import *

class MySQLManagerBasicTest(unittest.TestCase):
	def setUp(self):
		db = MySQLdb.connect(host="127.0.0.1", user="root", db="test")
		self.manager = MySQLManager(db=db, source="basic_table")

	def test_delete_all(self):
		print "Testing delete all"
		self.assertTrue(self.manager.remove())
		self.manager.commit()


	def test_insert(self):
		print "Testing insert"
		# remove all rows
		self.assertTrue(self.manager.remove())
		self.manager.commit()
		# Test findall, findone
		for i in range(10):
			self.manager.insert({"name": "jude" + str(i), "number": i, "datetime": datetime.now(), "date": date.today(), "double_number": 4.33333})
		self.manager.commit()

		result = self.manager.find({})
		self.assertTrue(len(result) >= 10)
		result_one = self.manager.find_one()
		self.assertTrue(result_one is not None)

		# Test conditioned query
		for i in range(10):
			result_conditioned = self.manager.find({"name": "jude" + str(i)})
			self.assertTrue(len(result) / len(result_conditioned) == 10)

			result_conditioned = self.manager.find({"number": i})
			self.assertTrue(len(result) / len(result_conditioned) == 10)

	def test_delete(self):
		print "Testing delete"
		self.manager.remove()
		self.manager.commit()
		for i in range(10):
			self.manager.insert({"name": "jude" + str(i), "number": i, "datetime": datetime.now(), "date": date.today(),  "double_number": 2.33333})
		self.manager.commit()
		for i in range(10):
			self.manager.remove({"number": i})
			self.manager.commit()
			all_result = self.manager.find()
			self.assertTrue(len(all_result) == 10 - i - 1)

		self.manager.commit()

	def test_update(self):
		print "Testing update"
		self.manager.remove()
		self.manager.commit()
		for i in range(10):
			self.manager.insert({"name": "jude" + str(i), "number": i, "datetime": datetime.now(), "date": date.today(),  "double_number": 2.33333})
		self.manager.commit()
		for i in range(10):
			self.manager.update(query={"number": i}, attributes={"number": i - 100})
			self.assertTrue(len(self.manager.find({"number": i})) == 0)
			self.assertTrue(len(self.manager.find({"number": i - 100})) == 1)
		self.manager.commit()

if __name__ == '__main__':
    unittest.main()