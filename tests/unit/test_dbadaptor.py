import unittest
import shutil
from contextlib import closing
from pathlib import Path
import sys
# give the test runner the import access
sys.path.insert(0, Path(__file__).parents[1].absolute())
from pysixdesk.lib import dbadaptor


class SQLDatabaseAdaptorTest(unittest.TestCase):

    def setUp(self):
        # prepare a testing folder
        self.test_folder = Path('unit_test/dbadaptor/')
        self.test_folder.mkdir(parents=True, exist_ok=True)
        self.db_name = str(self.test_folder / 'test.db')
        self.db = dbadaptor.SQLDatabaseAdaptor()
        self.conn = self.db.new_connection(self.db_name)
        self.name = 'unit_test'

    def test_sqldb(self):
        columns = {'a': 'INT', 'b': 'DOUBLE', 'c': 'BLOB', 'd': 'TEXT', 'e': 'NULL'}
        keys = {'autoincrement': ['a'],
                'primary': ['a', 'b']}
                # 'foreign': {'f': 'INT'}} # I don't know what this does
        self.db.create_table(self.conn, self.name, columns, keys, recreate=False)
        with closing(self.conn.cursor()) as c:
            c.execute(f'PRAGMA table_info({self.name});')
            out = c.fetchall()
        self.assertEqual(out[0], (0, 'a', 'INTEGER', 0, None, 1))
        self.assertEqual(out[1], (1, 'b', 'DOUBLE', 0, None, 2))
        self.assertEqual(out[2], (2, 'c', 'BLOB', 0, None, 0))
        self.assertEqual(out[3], (3, 'd', 'TEXT', 0, None, 0))
        self.assertEqual(out[4], (4, 'e', '', 0, None, 0))

        out = self.db.fetch_tables(self.conn)
        self.assertEqual(out, [(self.name,)])

        data = {'a': 1, 'b': 1.23, 'c': b'blabla', 'd': 'blabla', 'e': ''}
        self.db.insert(self.conn, self.name, data)
        with closing(self.conn.cursor()) as c:
            c.execute(f'SELECT * FROM {self.name};')
            out = c.fetchall()
        self.assertEqual(out, [tuple([v for v in data.values()])])

        data_m = {'a': [2, 3, 4, 5, 6],
                  'b': [2.23, 3.34, 4.45, 5.56, 6.67],
                  'c': [b'blabla']*5,
                  'd': ['blabla']*5,
                  'e': ['']*5}
        self.db.insertm(self.conn, self.name, data_m)
        with closing(self.conn.cursor()) as c:
            c.execute(f'SELECT * FROM {self.name};')
            out = c.fetchall()

        self.assertEqual(out[1], (2, 2.23, b'blabla', 'blabla', ''))
        self.assertEqual(out[2], (3, 3.34, b'blabla', 'blabla', ''))
        self.assertEqual(out[3], (4, 4.45, b'blabla', 'blabla', ''))
        self.assertEqual(out[4], (5, 5.56, b'blabla', 'blabla', ''))
        self.assertEqual(out[5], (6, 6.67, b'blabla', 'blabla', ''))

        out_select = self.db.select(self.conn, self.name)
        self.assertEqual(out_select, out)

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.test_folder.parents[0], ignore_errors=True)


class MySQLDatabaseAdaptorTest(unittest.TestCase):

    def setUp(self):
        # prepare a testing folder
        self.db = dbadaptor.MySQLDatabaseAdaptor()
        self.host = 'dbod-gc023'
        self.port = 5500
        self.user = 'admin'
        self.passwd = 'pysixdesk'
        self.db_name = 'unit_test'
        self.db.create_db(self.host,
                          self.user,
                          self.passwd,
                          self.db_name,
                          port=self.port)
        self.conn = self.db.new_connection(self.host,
                                           self.user,
                                           self.passwd,
                                           self.db_name,
                                           port=self.port)
        self.name = 'unit_test'

    def test_mysqldb(self):
        columns = {'a': 'INT', 'b': 'DOUBLE', 'c': 'BLOB', 'd': 'TEXT'}  # , 'e': 'NULL'} NULL causses an error
        keys = {'autoincrement': ['a'],
                'primary': ['a', 'b']}
                # 'foreign': {'f': 'INT'}} # I don't know what this does
        self.db.create_table(self.conn, self.name, columns, keys, recreate=False)
        with closing(self.conn.cursor()) as c:
            c.execute(f'SHOW COLUMNS FROM {self.name};')
            out = c.fetchall()

        self.assertEqual(out[0], ('a', 'int(11)', 'NO', 'PRI', None, 'auto_increment'))
        self.assertEqual(out[1], ('b', 'double', 'NO', 'PRI', None, ''))
        self.assertEqual(out[2], ('c', 'blob', 'YES', '', None, ''))
        self.assertEqual(out[3], ('d', 'text', 'YES', '', None, ''))

        out = self.db.fetch_tables(self.conn)
        self.assertEqual(out, [(self.name,)])

        data = {'a': 1, 'b': 1.23, 'c': b'blabla', 'd': 'blabla'}  # , 'e': ''}
        self.db.insert(self.conn, self.name, data)
        with closing(self.conn.cursor()) as c:
            c.execute(f'SELECT * FROM {self.name};')
            out = c.fetchall()
        self.assertEqual(out, (tuple([v for v in data.values()]),))

        data_m = {'a': [2, 3, 4, 5, 6],
                  'b': [2.23, 3.34, 4.45, 5.56, 6.67],
                  'c': [b'blabla']*5,
                  'd': ['blabla']*5}
                  # 'e': ['']*5}
        self.db.insertm(self.conn, self.name, data_m)
        with closing(self.conn.cursor()) as c:
            c.execute(f'SELECT * FROM {self.name};')
            out = c.fetchall()
        self.assertEqual(out[1], (2, 2.23, b'blabla', 'blabla'))
        self.assertEqual(out[2], (3, 3.34, b'blabla', 'blabla'))
        self.assertEqual(out[3], (4, 4.45, b'blabla', 'blabla'))
        self.assertEqual(out[4], (5, 5.56, b'blabla', 'blabla'))
        self.assertEqual(out[5], (6, 6.67, b'blabla', 'blabla'))

        out_select = self.db.select(self.conn, self.name)
        self.assertEqual(out_select, out)

    def tearDown(self):
        self.conn.close()
        with self.conn.cursor() as c:
            sql = "DROP DATABASE unit_test;"
            c.execute(sql)


if __name__ == '__main__':
    unittest.main()
