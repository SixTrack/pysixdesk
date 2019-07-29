import unittest
import os
import shutil
import time
from ..context import pysixdesk


class SqlDB(unittest.TestCase):
    def setUp(self):
        self.ws = pysixdesk.WorkSpace('unittest')

    def test_sql_study(self):
        self.ws.init_study('sql')
        self.assertEqual(self.ws.studies, ['sql'])

        self.st = self.ws.load_study('sql', module_path=os.path.join(os.path.dirname(__file__), 'config.py'))
        self.assertEqual(self.st.db_info['db_type'], 'sql')

        self.st.update_db()

        self.st.prepare_preprocess_input()
        in_files = os.listdir(os.path.join(self.st.study_path,
                                           'preprocess_input'))
        out_folders = os.listdir(os.path.join(self.st.study_path,
                                              'preprocess_output'))
        self.assertEqual(in_files, ['sub.db', 'db.ini', 'job_id.list', 'htcondor_run.sub'])
        self.assertEqual(out_folders, ['1', '2', '3', '4'])

        self.st.submit(0)
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)), 4)

        print('waiting for preprocess job to finish...')
        while len(self.st.submission.check_running(self.st.study_path)) >= 1:
            # sleep for 5 mins
            time.sleep(60*5)
        # add check on output of preprocess job

        self.prepare_sixtrack_input()
        # add assert here
        self.submit(1)
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)), 8)
        while len(self.st.submission.check_running(self.st.study_path)) >= 1:
            # sleep for 5 mins
            time.sleep(60*5)
        # add a check on the output of the sixtrack job

    def tearDown(self):
        # remove directory
        shutil.rmtree('unittest', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
