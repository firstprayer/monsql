import unittest
import MySQLdb
import sys
from datetime import *
from wrapper import *
import uuid
import time

class MySQLManagerBasicTest(unittest.TestCase):
	def setUp(self):
		db = MySQLdb.connect(host="127.0.0.1", user="root", passwd='root', db="test")
		self.manager = MySQLManager(db=db, source="basic_table")

	def test_delete_all(self):
		print "Testing delete all"
		self.assertTrue(self.manager.remove())
		self.manager.commit()

	def _insert_some_row_to_table_one(self, count):
		for i in range(count):
			self.manager.insert({"name": "jude" + str(i), "number": i, "datetime": datetime.now(), "date": date.today(), "double_number": 4.33333})
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

		self.manager.remove()
		self.manager.commit()

	def test_sorting(self):
		self._insert_some_row_to_table_one(10)

		data = self.manager.find({}, sort=[('number', DESCENDING)]).values()

		for i in range(len(data) - 1):
			self.assertTrue(data[i]['number'] > data[i + 1]['number'])

		self.manager.remove()
		self.manager.commit()
	def tearDown(self):
	 	self.manager.commit()


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

		self.manager.remove()
		self.manager.commit()

	# def test_table_join(self):
	# 	random_id = str(uuid.uuid1());


	# 	int_id = int(round(time.time() * 1000)) #(datetime.now() - datetime(1970, 1, 1)).total_seconds()

	# 	self.manager.set_source("basic_table")
	# 	self.manager.insert({"name": random_id, "id": int_id, "number": int_id, "datetime": datetime.now(), "date": date.today(), "double_number": 1})
	# 	self.manager.commit()

	# 	self.manager.set_source("basic_table_2")
	# 	self.manager.insert({"name": random_id, "id": int_id})
	# 	self.manager.commit()

	# 	self.manager.set_source("basic_table inner join basic_table_2")

	# 	result = self.manager.find(filter={"basic_table.name": F("basic_table_2.name")}, fields=["basic_table.name"])
	# 	result_of_table_2 = self.manager.set_source("basic_table_2").find()
	# 	print len(result)
	# 	print len(result_of_table_2)
	# 	self.assertTrue(len(result) == len(result_of_table_2))

	# 	self.manager.remove()
	# 	self.manager.commit()

	# def test_subquery(self):
	# 	random_id = str(uuid.uuid1());
	# 	int_id = (datetime.now() - datetime(1970, 1, 1)).total_seconds()

	# 	self.manager.set_source("basic_table")
	# 	self.manager.insert({"name": random_id, "id": int_id, "number": int_id, "datetime": datetime.now(), "date": date.today(), "double_number": 1})
	# 	self.manager.commit()

	# 	self.manager.set_source("basic_table_2")
	# 	self.manager.insert({"name": random_id, "id": int_id})
	# 	self.manager.commit()

	# 	query = Query(source="basic_table", filter={"name": random_id}, fields=['name'], alias="temp");
	# 	self.manager.set_source(query)

	# 	result = self.manager.find(fields=['name'])
	# 	# print result
	# 	self.assertTrue(len(result) == 1)

if __name__ == '__main__':
    unittest.main()