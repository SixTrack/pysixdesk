import unittest
import os
import shutil
import sys
from pathlib import Path
# give the test runner the import access
pysixdesk_path = str(Path(__file__).parents[3].absolute())
sys.path.insert(0, pysixdesk_path)
# setting environment variable for htcondor job.
if 'PYTHONPATH' in os.environ.keys():
    os.environ['PYTHONPATH'] = f"{pysixdesk_path}:{os.environ['PYTHONPATH']}"
else:
    os.environ['PYTHONPATH'] = f"{pysixdesk_path}"
import pysixdesk

from .test_mysql import MySqlStudy


class MySqlDBColl(MySqlStudy, unittest.TestCase):
    def setUp(self):
        self.test_folder = Path('integration_test/mysql_coll')
        self.test_folder.mkdir(parents=True, exist_ok=True)
        self.ws_name = 'integration_test'
        self.ws = pysixdesk.WorkSpace(str(self.test_folder / self.ws_name))
        self.st_name = 'mysql_coll_params'
        self.st = None

    def test_mysql_study(self):
        self.mysql_study(config='MySqlCollConfig')
        # add additional checks

    # def tearDown(self):
    #     # need to remove database
    #     if self.st is not None and self.st.db_info['db_type'] == 'mysql':
    #         conn = self.st.db.conn
    #         with conn.cursor() as c:
    #             sql = f"DROP DATABASE admin_{self.ws_name}_{self.st_name};"
    #             c.execute(sql)

    #     # remove directory
    #     shutil.rmtree('integration_test', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
