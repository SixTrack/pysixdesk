import unittest
import os
import shutil
import time
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


class SQLiteStudy:
    '''Runs a typical SqlLite test.
    Note, this method is not supposed to run on it's own, it must be subclassed
    and combined with the unittest.TestCase class for the self.assertEqual,
    self.assertTrue, and other attributes/methods to be defined.
    '''
    def sqlite_study(self, config="SQLiteConfig"):
        self.ws.init_study(self.st_name)
        self.assertTrue(self.st_name in self.ws.studies)

        self.st = self.ws.load_study(self.st_name,
                                     module_path=str(Path(__file__).parents[1] /
                                                     'variable_config.py'),
                                     class_name=config)
        self.assertEqual(self.st.db_info['db_type'], 'sql')

        self.st.update_db()

        self.st.prepare_preprocess_input()
        in_files = set(os.listdir(Path(self.st.study_path) / 'preprocess_input'))
        out_folders = set(os.listdir(Path(self.st.study_path) / 'preprocess_output'))
        self.assertEqual(in_files, set(['sub.db',
                                        'input.ini',
                                        'job_id.list',
                                        'htcondor_run.sub']))
        # getting the expected list of preprocess wu_ids.
        where = "status='incomplete'"
        wu_ids = self.st.db.select('preprocess_wu', ['wu_id'], where)
        wu_ids = [str(o[0]) for o in wu_ids]
        self.assertEqual(out_folders, set(wu_ids))

        self.st.submit(0)
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)),
                         len(wu_ids))

        print('waiting for preprocess job to finish...')
        while self.st.submission.check_running(self.st.study_path) is None\
                or len(self.st.submission.check_running(self.st.study_path)) >= 1:
            time.sleep(30)
        # TODO: add a check on the output of the preprocess job

        self.st.collect_result(0)
        # TODO: add check on result collection

        self.st.prepare_sixtrack_input()

        # getting the expected list of sixtrack wu_ids.
        where = "status='complete'"
        pre_wu_ids = self.st.db.select('preprocess_wu', ['wu_id'], where)
        pre_wu_ids = tuple([p[0] for p in pre_wu_ids])
        if len(pre_wu_ids) == 1:
            where = f"status='incomplete' and preprocess_id={pre_wu_ids[0]}"
        else:
            where = f"status='incomplete' and preprocess_id in {pre_wu_ids}"
        six_wu_ids = self.st.db.select('sixtrack_wu', ['wu_id'], where)
        six_wu_ids = [s[0] for s in six_wu_ids]
        # TODO: add assert here

        self.st.submit(1)
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)),
                         len(six_wu_ids))

        print('waiting for sixtrack job to finish...')
        while self.st.submission.check_running(self.st.study_path) is None\
                or len(self.st.submission.check_running(self.st.study_path)) >= 1:
            time.sleep(60)
        # TODO: add a check on the output of the sixtrack job

        self.st.collect_result(1)
        # TODO: add check on result collection


class SQLiteDB(SQLiteStudy, unittest.TestCase):
    def setUp(self):
        self.test_folder = Path('integration_test/sqllite')
        self.test_folder.mkdir(parents=True, exist_ok=True)
        self.ws_name = 'integration_test'
        self.ws = pysixdesk.WorkSpace(str(self.test_folder / self.ws_name))
        self.st_name = 'sqlite'
        self.st = None

    def test_sqlite_study(self):
        self.sqlite_study(config='SQLiteConfig')

    def tearDown(self):
        # remove directory
        shutil.rmtree('integration_test', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
