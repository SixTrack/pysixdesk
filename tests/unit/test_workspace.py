import unittest
import shutil
from pathlib import Path
import sys
# give the test runner the import access
pysixdesk_path = str(Path(__file__).parents[2].absolute())
sys.path.insert(0, pysixdesk_path)
from pysixdesk.lib import workspace


class WorkSpaceTest(unittest.TestCase):

    def setUp(self):
        self.test_folder = Path('unit_test/workspace/')
        self.test_folder.mkdir(parents=True, exist_ok=True)
        self.ws = workspace.WorkSpace(str(self.test_folder / 'unit_test_ws'))
        self.st = None

    def test_init_load(self):
        paths_out = {'workspace': str((self.test_folder / 'unit_test_ws').absolute()),
                     'studies'  : str((self.test_folder / 'unit_test_ws/studies').absolute()),
                     'templates': str((self.test_folder / 'unit_test_ws/templates').absolute())}
        self.assertEqual(self.ws.studies, [])
        self.assertEqual(self.ws.paths, paths_out)

        self.ws.init_study('unit_test_st')
        paths_out = {'workspace': str((self.test_folder / 'unit_test_ws').absolute()),
                     'studies'  : str((self.test_folder / 'unit_test_ws/studies').absolute()),
                     'templates': str((self.test_folder / 'unit_test_ws/templates').absolute())}
        self.assertEqual(self.ws.studies, ['unit_test_st'])
        self.assertEqual(self.ws.paths, paths_out)

        # default config.py
        self.st = self.ws.load_study('unit_test_st')

        load_tables = set([('boinc_vars',),
                           ('collimation_results',),
                           ('env',),
                           ('oneturn_sixtrack_result',),
                           ('oneturn_sixtrack_wu',),
                           ('preprocess_task',),
                           ('preprocess_wu',),
                           ('six_results',),
                           ('sixtrack_task',),
                           ('sixtrack_wu',),
                           ('templates',)])
        self.assertEqual(set(self.st.db.fetch_tables()), load_tables)
        # is there a better way to test this ?
        self.assertIsNotNone(self.st)

    def tearDown(self):
        if self.st is not None and self.st.db_info['db_type'] == 'mysql':
            conn = self.st.db.conn
            with conn.cursor() as c:
                sql = "DROP DATABASE unit_test_ws_unit_test_st;"
                c.execute(sql)

        shutil.rmtree(self.test_folder.parents[0], ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
