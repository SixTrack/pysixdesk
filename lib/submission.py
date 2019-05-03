import os
import sys
import utils
import subprocess
import shutil
import study

from abc import ABC, abstractmethod
from subprocess import Popen, PIPE

class Cluster(ABC):

    def __init__(self, study):
        '''Constructor'''
        self.study = study

    @abstractmethod
    def prepare(self):
        pass

    @abstractmethod
    def submit(self):
        pass

    @abstractmethod
    def status(self):
        pass

    @abstractmethod
    def remove(self):
        pass

class HTCondor(Cluster):
    '''The HTCondor management system'''

    def __init__(self, study):
        '''Constructor'''
        super(HTCondor, self).__init__(study)

    def prepare(self, wu_ids, trans, exe, exe_args, input_path, output_path):
        '''Prepare the submission file
        @wu_ids(tuple) The job ids for submission
        @trans(list) The python modules needed by the executables
        @exe(str) The executable
        @exe_args(str) The arguments for executable
        @input_path(str) The folder with input files
        @output_path(str) The output folder'''

        app_path = study.StudyFactory.app_path()
        job_list = os.path.join(input_path, 'job_id.list')
        if os.path.exists(job_list):
            os.remove(job_list)
        with open(job_list, 'w') as f_out:
            for i in wu_ids:
                f_out.write(str(i))
                f_out.write('\n')
                out_f = os.path.join(output_path, str(i))
                if os.path.exists(out_f):
                    shutil.rmtree(out_f)
                os.makedirs(out_f)
        os.chmod(job_list, 0o444)#change the permission to readonly
        rep = {}
        rep['%func'] = utils.evlt(utils.encode_strings, [trans])
        rep['%exe'] = exe
        rep['%dirname'] = output_path
        rep['%joblist'] = job_list
        rep['%input'] = exe_args
        sub_temp = os.path.join(self.study.paths['templates'], 'htcondor_run.sub')
        sub_file = os.path.join(input_path, 'htcondor_run.sub')
        if os.path.exists(sub_file):
            os.remove(sub_file)#remove the old one
        with open(sub_temp, 'r') as f_in:
            with open(sub_file, 'w') as f_out:
                conts = f_in.read()
                for key, value in rep.items():
                    conts = conts.replace(key, value)
                f_out.write(conts)
        print("The htcondor description file is ready!")

    def submit(self, input_path, job_name, trials=5, **args):
        '''Submit the job to the cluster'''
        sub = os.path.join(input_path, 'htcondor_run.sub')
        joblist = os.path.join(input_path, 'job_id.list')
        if not os.path.isfile(joblist):
            print("There isn't %s job for submission!"%job_name)
            return False
        scont = 1
        while scont <= trials:
            process = Popen(['condor_submit', sub], stdout=PIPE,\
                    stderr=PIPE, universal_newlines=True)
            stdout, stderr = process.communicate()
            if stderr:
                print(stdout)
                print(stderr)
                scont += 1
            else:
                print(stdout)
                return True
        return False

    def status(self, **args):
        pass

    def remove(self, jobid):
        '''Cancel the submitted jobs'''
        pass
