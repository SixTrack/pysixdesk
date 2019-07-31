import unittest
import shutil
import os
from pathlib import Path
import sys
# give the test runner the import access
sys.path.insert(0, Path(__file__).parents[1].absolute())
from pysixdesk.lib import submission


class SubmissionTest(unittest.TestCase):

    def setUp(self):
        # prepare a testing folder
        self.sub_folder_in = Path('unit_test/submission/in')
        self.sub_folder_in.mkdir(parents=True, exist_ok=True)
        self.sub_folder_out = Path('unit_test/submission/out')
        self.sub_folder_out.mkdir(parents=True, exist_ok=True)
        self.cluster = submission.HTCondor(temp_path=Path(__file__).parents[2] / 'templates/')
        self.wu_ids = [1, 2, 3, 4]
        self.exe = str((self.sub_folder_in / 'dummy.exe').absolute())
        with open(self.exe, 'w') as f:
            f.write('#!/bin/bash\n')
            f.write('sleep 10\n')
        self.trans = [str((self.sub_folder_in / 'db.ini').absolute())]
        for p in self.trans:
            open(p, 'w').close()
        self.jobs = None

    def test_prepare_submit(self):
        exe_args = 'dummyarg'
        input_path = str(self.sub_folder_in.absolute())
        output_path = str(self.sub_folder_out.absolute())
        self.cluster.prepare(self.wu_ids,
                             self.trans,
                             self.exe,
                             exe_args,
                             input_path,
                             output_path)
        contents = {}
        with open(self.sub_folder_in / 'htcondor_run.sub') as f:
            for line in f:
                if '=' in line:
                    line = line.split('=')
                    contents[line[0].strip()] = line[1].strip()
        self.assertEqual(contents['transfer_input_files'], ','.join(self.trans))
        self.assertEqual(contents['executable'], self.exe)
        self.assertEqual(contents['arguments'], f'$(wu_id) {exe_args}')
        self.assertEqual(contents['initialdir'], f'{output_path}/$(wu_id)/')
        self.assertEqual(contents['output'], f'{output_path}/$(wu_id)/htcondor.$(ClusterId).$(ProcId).out')
        self.assertEqual(contents['error'], f'{output_path}/$(wu_id)/htcondor.$(ClusterId).$(ProcId).err')
        self.assertEqual(contents['log'], f'{output_path}/$(wu_id)/htcondor.$(ClusterId).$(ProcId).log')

        status, out = self.cluster.submit(self.sub_folder_in, 'unit_test_submission')
        self.jobs = out
        self.assertTrue(status)
        self.assertEqual(list(out.keys()), [str(i) for i in self.wu_ids])

    def tearDown(self):
        # remove jobs if they were submitted
        if self.jobs is not None:
            for j in self.jobs.values():
                p = os.popen(f'condor_rm {j}')
                p.close()
        # remove testing folder
        shutil.rmtree(self.sub_folder_in.parents[1], ignore_errors=True)
        shutil.rmtree(self.sub_folder_out.parents[1], ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
