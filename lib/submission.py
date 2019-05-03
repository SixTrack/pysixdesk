import os
import sys
import utils
import subprocess
import shutil
import study

from abc import ABC, abstractmethod
from subprocess import Popen, PIPE

class Cluster(ABC):

    def __init__(self):
        '''Constructor'''
        pass

    @abstractmethod
    def prepare(self):
        pass

    @abstractmethod
    def submit(self):
        pass

    @abstractmethod
    def check(self):
        pass

    @abstractmethod
    def remove(self):
        pass

class HTCondor(Cluster):
    '''The HTCondor management system'''

    def __init__(self, temp_path=None):
        '''Constructor'''
        self.temp = temp_path
        self.sub_name = 'htcondor_run.sub'

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
        if self.temp is None:
            self.temp = os.path.basename(input_path)
        sub_temp = os.path.join(self.temp, self.sub_name)
        sub_file = os.path.join(input_path, self.sub_name)
        if os.path.exists(sub_file):
            os.remove(sub_file)#remove the old one
        with open(sub_temp, 'r') as f_in:
            with open(sub_file, 'w') as f_out:
                conts = f_in.read()
                for key, value in rep.items():
                    conts = conts.replace(key, value)
                f_out.write(conts)
        print("The htcondor description file is ready!")

    def submit(self, input_path, job_name, trials=5, *args, **kwargs):
        '''Submit the job to the cluster'''
        sub = os.path.join(input_path, self.sub_name)
        joblist = os.path.join(input_path, 'job_id.list')
        if not os.path.isfile(joblist):
            print("There isn't %s job for submission!"%job_name)
            return False
        scont = 1
        while scont <= trials:
            args = list(args)
            for ky, vl in kwargs:
                args = args + ['-'+ky, vl]
            args.append(sub)
            process = Popen(['condor_submit', *args], stdout=PIPE,\
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

    def check(self, *args, **kwargs):
        '''Check the job status'''
        for ky, vl in kwargs:
            args = args + ['-'+ky, vl]
        process = Popen(['condor_q', *args], stdout=PIPE,\
                stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            print(stdout)
            print(stderr)
        else:
            print(stdout)

    def remove(self, **args):
        '''Cancel the submitted jobs'''
        for ky, vl in kwargs:
            args = args + ['-'+ky, vl]
        process = Popen(['condor_rm', sub], stdout=PIPE,\
                stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            print(stdout)
            print(stderr)
        else:
            print(stdout)
