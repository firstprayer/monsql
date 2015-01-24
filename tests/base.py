import unittest
import uuid
from monsql import MonSQL, DB_TYPES
import yaml
import os

def random_name():
    uids = str(uuid.uuid1()).split('-')
    return uids[1] + uids[3];

def load_test_settings():
    config_path = os.path.join(os.path.join(os.path.dirname(__file__), 'config.yml'))
    if os.path.exists(config_path):
        return yaml.load(open(config_path).read())
    else:
        return os.environ

class BaseTestCase(unittest.TestCase):
    # STATIC
    DB_TYPE = None

    # INSTANCE VARIABLES
    monsql = None

    def setUp(self):
        dbtype = os.environ['DB_TYPE']
        config_data = load_test_settings()

        if dbtype == DB_TYPES.MYSQL:
            self.monsql = MonSQL(host=config_data['MYSQL_HOST'], 
                                 port=int(config_data['MYSQL_PORT']), 
                                 username=config_data['MYSQL_USER'],
                                 password=config_data['MYSQL_PASSWORD'], 
                                 dbname=config_data['MYSQL_DB'], 
                                 dbtype=dbtype)

        elif dbtype == DB_TYPES.SQLITE3:
            self.monsql = MonSQL(dbpath=config_data['SQLITE3_PATH'],
                                 dbtype=dbtype)
        elif dbtype == DB_TYPES.POSTGRESQL:
            self.monsql = MonSQL(host=config_data['POSTGRESQL_HOST'], 
                                 port=config_data['POSTGRESQL_PORT'], 
                                 username=config_data['POSTGRESQL_USER'],
                                 password=config_data['POSTGRESQL_PASSWORD'], 
                                 dbname=config_data['POSTGRESQL_DB'], 
                                 dbtype=dbtype)
        else:
            raise Exception('dbtype is incorrect')


    def tearDown(self):
        self.monsql.close()
