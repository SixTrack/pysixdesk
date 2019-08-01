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
                                     module_path=str(Path(__file__).parents[1] /
                                                     'variable_config.py'),
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
        # getting the expected list of preprocess wu_ids.
        where = "status='incomplete'"
        wu_ids = self.st.db.select('preprocess_wu', ['wu_id'], where)
        wu_ids = [str(o[0]) for o in wu_ids]
        self.assertEqual(out_folders, wu_ids)

        self.st.submit(0)
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)),
                         len(wu_ids))

        print('waiting for preprocess job to finish...')
        while self.st.submission.check_running(self.st.study_path) is None\
                or len(self.st.submission.check_running(self.st.study_path)) >= 1:
            # sleep for 5 mins
            time.sleep(10)
        # TODO: add a check on the output of the preprocess job

<<<<<<< HEAD
        self.st.prepare_sixtrack_input()
        # getting the expected list of sixtrack wu_ids.
        # TODO: test this !
        where = "status='complete'"
        pre_wu_ids = self.st.db.select('preprocess_wu', ['wu_id'], where)
=======
        self.prepare_sixtrack_input()
        # getting the expected list of sixtrack wu_ids.
        # TODO: test this !
        where = "status='complete'"
        pre_wu_ids = self.db.select('preprocess_wu', ['wu_id'], where)
>>>>>>> 8a7a303... improved integration tests
        pre_wu_ids = [p[0] for p in pre_wu_ids]
        if len(pre_wu_ids) == 1:
            where = "status='incomplete' and preprocess_id=%s" % str(pre_wu_ids[0])
        else:
            where = "status='incomplete' and preprocess_id in %s" % str(pre_wu_ids)
<<<<<<< HEAD
        six_wu_ids = self.st.db.select('sixtrack_wu', ['wu_id'], where)
        six_wu_ids = [s[0] for s in six_wu_ids]
        # TODO: add assert here

        self.st.submit(1)
=======
        six_wu_ids = self.db.select('sixtrack_wu', ['wu_id'], where)
        six_wu_ids = [s[0] for s in six_wu_ids]
        # TODO: add assert here

        self.submit(1)
>>>>>>> 8a7a303... improved integration tests
        self.assertEqual(len(self.st.submission.check_running(self.st.study_path)),
                         len(six_wu_ids))

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
