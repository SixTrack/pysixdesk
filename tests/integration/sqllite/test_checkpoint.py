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

from .test_sqllite import SqlLiteStudy


class MySqlDBCheckpoint(SqlLiteStudy, unittest.TestCase):
    def setUp(self):
        self.test_folder = Path('integration_test/sqllite_checkpoint')
        self.test_folder.mkdir(parents=True, exist_ok=True)
        self.ws_name = 'integration_test'
        self.ws = pysixdesk.WorkSpace(str(self.test_folder / self.ws_name))
        self.st_name = 'sqllite_checkpoint'
        self.st = None

    def test_sqllite_study_restart(self):
        # run a normal sqlite study. with the correct executable.
        self.sqllite_study(config='SqlLiteCheckpointConfig')
        # continue previous study
        self.st = self.ws.load_study(self.st_name,
                                     module_path=str(Path(__file__).parents[1] /
                                                     'variable_config.py'),
                                     class_name='SqlLiteCheckpointConfig')
        self.assertTrue(self.st.checkpoint_restart)

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
            time.sleep(10)
        # TODO: add a check on the output of the sixtrack job

        # add additional check on the checkpointing.

    def tearDown(self):
        # remove directory
        shutil.rmtree('integration_test', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
