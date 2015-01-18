
from tests import base
import os
import unittest

if __name__ == '__main__':

    config_data = base.load_test_settings()
    suite = unittest.TestLoader().discover('tests') 
    # suite = [unittest.TestLoader().loadTestsFromTestCase(case) for case in [tests.test_table.MonSQLBasicTest]]

    runner = unittest.TextTestRunner(verbosity=2)
    for dbtype in config_data['TARGET_DATABASES'].split(','):
        print "Testing %s" %dbtype
        os.environ['DB_TYPE'] = dbtype
        runner.run(suite)
