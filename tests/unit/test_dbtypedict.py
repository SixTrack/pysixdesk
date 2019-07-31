import unittest
import sys
from pathlib import Path
# give the test runner the import access
sys.path.insert(0, Path(__file__).parents[1].absolute())
from pysixdesk.lib import dbtypedict


class DBTypeDicTest(unittest.TestCase):
    def setUp(self):
        self.sql_dict = dbtypedict.SQLiteDict()
        self.mysql_dict = dbtypedict.MySQLDict()

    def test_dbtypedict_sql_int(self):
        value = 123
        self.assertEqual(self.sql_dict[value], 'INT')
        value = int(1.23e5)
        self.assertEqual(self.sql_dict[value], 'INT')

    def test_dbtypedict_sql_float(self):
        value = 1.23
        self.assertEqual(self.sql_dict[value], 'DOUBLE')
        value = 1.23e5
        self.assertEqual(self.sql_dict[value], 'DOUBLE')

    def test_dbtypedict_sql_str(self):
        value = 'blabla'
        self.assertEqual(self.sql_dict[value], 'TEXT')

    def test_dbtypedict_sql_bytes(self):
        value = b'blabla'
        self.assertEqual(self.sql_dict[value], 'BLOB')

    def test_dbtypedict_sql_tuple(self):
        value = (1, 2)
        self.assertEqual(self.sql_dict[value], 'INT')
        value = (1.1, 2.1)
        self.assertEqual(self.sql_dict[value], 'INT')

    def test_dbtypedict_sql_none(self):
        value = None
        self.assertEqual(self.sql_dict[value], 'NULL')

    def test_dbtypedict_mysql_int(self):
        value = 123
        self.assertEqual(self.mysql_dict[value], 'INT')
        value = int(1.23e5)
        self.assertEqual(self.mysql_dict[value], 'INT')

    def test_dbtypedict_mysql_float(self):
        value = 1.23
        self.assertEqual(self.mysql_dict[value], 'DOUBLE')
        value = 1.23e5
        self.assertEqual(self.mysql_dict[value], 'DOUBLE')

    def test_dbtypedict_mysql_str(self):
        value = 'blabla'
        self.assertEqual(self.mysql_dict[value], 'TEXT')

    def test_dbtypedict_mysql_bytes(self):
        value = b'blabla'
        self.assertEqual(self.mysql_dict[value], 'BLOB')

    def test_dbtypedict_mysql_tuple(self):
        value = (1, 2)
        self.assertEqual(self.mysql_dict[value], 'TEXT')
        value = (1.1, 2.1)
        self.assertEqual(self.mysql_dict[value], 'TEXT')

    def test_dbtypedict_mysql_none(self):
        value = None
        self.assertEqual(self.mysql_dict[value], 'NULL')


if __name__ == '__main__':
    unittest.main()
