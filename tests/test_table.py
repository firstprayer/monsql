import unittest
import sys
from datetime import *
import uuid
import time
from base import *
from monsql import MonSQL, DESCENDING, DB_TYPES
from monsql.exception import MonSQLException


class MonSQLBasicTest(BaseTestCase):
    
    def setUp(self):
        super(MonSQLBasicTest, self).setUp()

        test_table_name_a = 'test_a_' + random_name()

        self.monsql.create_table(test_table_name_a, ['name VARCHAR(50)', 'number INT', 'datetime timestamp', 'date DATE', 'double_number DOUBLE PRECISION'])
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
        self._insert_some_row_to_table_one(10)
        self._insert_some_row_to_table_one(10)

        query_set = self.table_a.find()
        self.assertEqual(query_set.count, 20)
        self.assertEqual(query_set.distinct().count, 10)
        self.assertEqual(query_set.filter({'number': 1}).count, 2)
        self.assertEqual(query_set.filter({'number': 1}).filter({'name': 'jude1'}).count, 2)

        self.assertEqual(query_set.filter({'number': 1}).filter({'name': 'jude1'}).distinct().count, 1)
        self.assertEqual(query_set.filter({'number': 1}).filter({'name': 'jude0'}).count, 0)

        self.assertEqual(self.table_a.find(fields=['date']).distinct().count, 1)

    def test_raw(self):
        self._insert_some_row_to_table_one(10)
        rows = self.monsql.raw('select * from %s' %self.table_a.name)
        for idx, row in enumerate(rows):
            self.assertEqual(row.number, idx)
        
        self.monsql.raw('update %s set number=100000' %self.table_a.name)
        self.monsql.commit()

        self.assertEqual(self.table_a.find({'number': 1}).count, 0)
        self.assertEqual(self.table_a.find({'number': 100000}).count, 10)
        self.monsql.commit()


if __name__ == '__main__':
    config_data = load_test_settings()
    
    runner = unittest.TextTestRunner(verbosity=2)
    for dbtype in config_data['TARGET_DATABASES'].split(','):
        print "Testing %s" %dbtype
        os.environ['DB_TYPE'] = dbtype

        unittest.main()
