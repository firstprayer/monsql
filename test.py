import unittest
import sys
from datetime import *
import uuid
import time

from monsql import MonSQL, DESCENDING, DB_TYPES
from monsql.exception import MonSQLException

def random_name():
    uids = str(uuid.uuid1()).split('-')
    return uids[1] + uids[3];

DB_TYPE = None

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.monsql = MonSQL(host="127.0.0.1", port=3306, username="root", password='root', dbname="test", dbtype=DB_TYPE)

    def tearDown(self):
        self.monsql.close()

class MonSQLMetaOperationTest(BaseTestCase):
    
    def test_table_operations(self):
        tablename = 'test_table' + random_name()
        self.monsql.create_table(tablename, [('id INT NOT NULL')])
        self.assertTrue(self.monsql.is_table_existed(tablename))
        table_obj = self.monsql.get(tablename)

        for id in range(10):
            table_obj.insert({'id': id})
        self.monsql.commit()

        assert table_obj.count() == 10
        self.monsql.truncate_table(tablename)
        assert table_obj.count() == 0

        is_exception_raised = False
        
        try:
            self.monsql.create_table(tablename, [('id INT')], force_recreate=False)
        except MonSQLException as e:
            is_exception_raised = True
        self.assertTrue(is_exception_raised)

        self.monsql.create_table(tablename, [('id INT')], primary_key=['id'], force_recreate=True)
        assert self.monsql.is_table_existed(tablename)

        self.monsql.drop_table(tablename)
        self.assertFalse(self.monsql.is_table_existed(tablename))


class MonSQLBasicTest(BaseTestCase):
    
    def setUp(self):
        super(MonSQLBasicTest, self).setUp()

        test_table_name_a = 'test_a_' + random_name()
        # print test_table_name_a
        # test_table_name_b = 'test_b_' + random_name()

        self.monsql.create_table(test_table_name_a, ['name VARCHAR(50)', 'number INT', 'datetime DATETIME', 'date DATE', 'double_number DOUBLE'])
        self.table_a = self.monsql.get(test_table_name_a)

    def tearDown(self):
        self.monsql.drop_table(self.table_a.name)
        super(MonSQLBasicTest, self).tearDown()

    def test_delete_all(self):
        self._insert_some_row_to_table_one(10)
        assert self.table_a.count() == 10       

        self.table_a.remove()
        self.monsql.commit()

        assert self.table_a.count() == 0

    def _insert_some_row_to_table_one(self, count):
        for i in range(count):
            self.table_a.insert({"name": "jude" + str(i), "number": i, "datetime": datetime.now(), "date": date.today(), "double_number": 4.33333})
        self.monsql.commit()

    def test_complex_queries(self):
        self._insert_some_row_to_table_one(10)

        self.assertEquals(len(self.table_a.find({})), 10)

        filters_and_expected_row_nums = [
            ({'number': {'$gte': 1}}, 9),
            ({'number': {'$gt': 1}}, 8),
            ({'number': {'$lt': 1}}, 1),
            ({'number': {'$lte': 1}}, 2),
            ({'number': {'$eq': 1}}, 1),
            ({'number': {'$in': [1, 11]}}, 1),
            ({'name': {'$contains': 'de0'}}, 1),
        ]

        for filter, row_num in filters_and_expected_row_nums:
            self.assertEqual(len(self.table_a.find(filter)), row_num)
            self.assertEqual(len(self.table_a.find({'$not': filter})), 10 - row_num)

        composite_filters_and_expected_row_nums = [
            ({'$and': [{'name': 'jude0'}, {'number': 0}]}, 1),
            ({'$and': [{'name': 'jude0'}, {'number': 1}]}, 0),
            ({'$or': [{'name': 'jude0'}, {'number': 0}]}, 1),
            ({'$or': [{'name': 'jude0'}, {'number': 1}]}, 2),
        ]

        for filter, row_num in composite_filters_and_expected_row_nums:
            self.assertEqual(len(self.table_a.find(filter)), row_num)


    def test_insert(self):
        # Test findall, findone
        self._insert_some_row_to_table_one(10)

        result = self.table_a.find()
        self.assertTrue(len(result) == 10)
        result_one = self.table_a.find_one()
        self.assertTrue(result_one is not None)

        # Test conditioned query
        for i in range(10):
            result_conditioned = self.table_a.find({"name": "jude" + str(i)})
            self.assertTrue(len(result_conditioned) == 1)

            result_conditioned = self.table_a.find({"number": i})
            self.assertTrue(len(result_conditioned) == 1)

    def test_sorting(self):
        self._insert_some_row_to_table_one(10)

        data = self.table_a.find({}, sort=[('number', DESCENDING)])
        first_row = self.table_a.find_one(sort=[('number', DESCENDING)])
        for i in range(len(data) - 1):
            self.assertTrue(data[i].number > data[i + 1].number)

        assert first_row.number == max([r.number for r in data])

    def test_delete(self):
        self._insert_some_row_to_table_one(10)

        for i in range(10):
            self.table_a.remove({"number": i})
            self.table_a.commit()
            all_result = self.table_a.find()
            self.assertTrue(len(all_result) == 10 - i - 1)

    def test_update(self):
        self._insert_some_row_to_table_one(10)

        for i in range(10):
            self.table_a.update(query={"number": i}, attributes={"number": i - 100})
            self.assertTrue(len(self.table_a.find({"number": i})) == 0)
            self.assertTrue(len(self.table_a.find({"number": i - 100})) == 1)

    def test_limit(self):
        self._insert_some_row_to_table_one(10)

        full_rows = self.table_a.find().values()

        rows_with_limit = self.table_a.find(limit=5).values()
        rows_with_limit_and_skip = self.table_a.find(limit=5, skip=5).values()

        self.assertEqual(full_rows[: 5], rows_with_limit)
        self.assertEqual(full_rows[5: ], rows_with_limit_and_skip)

    def test_query_set(self):
        pass # TODO
    

if __name__ == '__main__':

    suites = [unittest.TestLoader().loadTestsFromTestCase(case) for case in \
             [MonSQLMetaOperationTest, MonSQLBasicTest]]

    runner = unittest.TextTestRunner(verbosity=1)
    DB_TYPE = DB_TYPES.MYSQL
    for suite in suites: runner.run(suite)

    DB_TYPE = DB_TYPES.SQLITE3
    for suite in suites: runner.run(suite)



