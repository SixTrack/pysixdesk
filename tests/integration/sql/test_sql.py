import unittest
import os
import shutil
import time
import sys
from pathlib import Path
# give the test runner the import access
sys.path.insert(0, Path(__file__).parents[2].absolute())
import pysixdesk
from pathlib import Path


class SqlDB(unittest.TestCase):
    def setUp(self):
        self.test_folder = Path('integration_test/sql')
        self.test_folder.mkdir(parents=True, exist_ok=True)
        self.ws_name = 'sql_ws'
        self.ws = pysixdesk.WorkSpace(str(self.test_folder / self.ws_name))
        self.st_name = 'sql_st'
        self.st = None

    def test_sql_study(self):
        self.ws.init_study(self.st_name)
        self.assertEqual(self.ws.studies, [self.st_name])

        self.st = self.ws.load_study(self.st_name,
                                     module_path=str(Path(__file__).parents[1] / 'variable_config.py'),
                                     class_name='SqlConfig')
        self.assertEqual(self.st.db_info['db_type'], 'sql')

        self.st.update_db()

        self.st.prepare_preprocess_input()
        in_files = os.listdir(Path(self.st.study_path) / 'preprocess_input')
        out_folders = os.listdir(Path(self.st.study_path) / 'preprocess_output')
        self.assertEqual(in_files, ['sub.db',
                                    'db.ini',
                                    'job_id.list',
                                    'htcondor_run.sub'])
        self.assertEqual(out_folders, ['1', '2', '3', '4'])

        self.st.submit(0)
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)), 4)

        print('waiting for preprocess job to finish...')
        while self.st.submission.check_running(self.st.study_path) is None\
                or len(self.st.submission.check_running(self.st.study_path)) >= 1:
            # sleep for 5 mins
            time.sleep(60*5)
        # TODO: add a check on the output of the preprocess job

        self.prepare_sixtrack_input()
        # TODO: add assert here
        self.submit(1)
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)), 8)

        print('waiting for sixtrack job to finish...')
        while self.st.submission.check_running(self.st.study_path) is None\
                or len(self.st.submission.check_running(self.st.study_path)) >= 1:
            # sleep for 5 mins
            time.sleep(60*5)
        # TODO: add a check on the output of the sixtrack job

    def tearDown(self):
        # remove directory
        shutil.rmtree('integration_test', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
