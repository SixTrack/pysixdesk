import os
import shutil
import logging

from abc import ABC, abstractmethod
from subprocess import Popen, PIPE

from . import utils


class Cluster(ABC):

    def __init__(self, temp_path):
        '''Constructor'''
        self._logger = logging.getLogger(__name__)

    @abstractmethod
    def prepare(self, wu_ids, trans, exe, exe_args, input_path, output_path,
                *args, **kwargs):
        pass

    @abstractmethod
    def submit(self, input_path, job_name, trials=5, *args, **kwargs):
        pass

    @abstractmethod
    def check(self, *args, **kwargs):
        pass

    @abstractmethod
    def remove(self, **kwargs):
        pass


class HTCondor(Cluster):
    '''The HTCondor management system'''

    def __init__(self, temp_path=None):
        '''Constructor'''
        super().__init__(temp_path)
        self.temp = temp_path
        self.sub_name = 'htcondor_run.sub'

    def prepare(self, wu_ids, trans, exe, exe_args, input_path, output_path,
                *args, **kwargs):
        '''Prepare the submission file
        @wu_ids(tuple) The job ids for submission
        @trans(list) The python modules needed by the executables
        @exe(str) The executable
        @exe_args(str) The additional arguments for executable except for wu_id
        @input_path(str) The folder with input files
        @output_path(str) The output folder
        @*args and **kwargs Other necessary arguments'''

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
        os.chmod(job_list, 0o444)  # change the permission to readonly
        # trans.append(os.path.join(utils.PYSIXDESK_ABSPATH, 'pysixdesk/lib', 'resultparser.py'))
        # trans.append(os.path.join(utils.PYSIXDESK_ABSPATH, 'pysixdesk/lib', 'utils.py'))
        # trans.append(os.path.join(utils.PYSIXDESK_ABSPATH, 'pysixdesk/lib', 'pysixdb.py'))
        # trans.append(os.path.join(utils.PYSIXDESK_ABSPATH, 'pysixdesk/lib', 'dbadaptor.py'))
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
            os.remove(sub_file)  # remove the old one
        with open(sub_temp, 'r') as f_in:
            with open(sub_file, 'w') as f_out:
                conts = f_in.read()
                for key, value in rep.items():
                    conts = conts.replace(key, value)
                f_out.write(conts)
        content = "The htcondor description file is ready!"
        self._logger.info(content)

    def submit(self, input_path, job_name, trials=5, *args, **kwargs):
        '''Submit the job to the cluster
        @input_path The input path to hold the input files
        @job_name The job name (also is the batch_name for HTCondor)
        @trails The maximum number of resubmission when submit failed'''
        sub = os.path.join(input_path, self.sub_name)
        joblist = os.path.join(input_path, 'job_id.list')
        if not os.path.isfile(joblist):
            content = "There isn't %s job for submission!" % job_name
            self._logger.warning(content)
            return False, None
        scont = 1
        while scont <= trials:
            args = list(args)
            for ky, vl in kwargs:
                args = args + ['-' + ky, vl]
            args.append(sub)
            args.append('-batch-name')
            args.append(job_name)
            process = Popen(['condor_submit', '-terse', *args], stdout=PIPE,
                            stderr=PIPE, universal_newlines=True)
            stdout, stderr = process.communicate()
            if stderr:
                self._logger.info(stdout)
                self._logger.error(stderr)
                scont += 1
            else:
                self._logger.info(stdout)
                outs = stdout.split()
                [cluster_id, proc_st] = outs[0].split('.')
                [cluster_id, proc_ed] = outs[-1].split('.')
                with open(joblist, 'r') as f_in:
                    wu_ids = f_in.read().split()
                try:
                    cl_id = int(cluster_id)
                    proc_st = int(proc_st)
                    proc_ed = int(proc_ed)
                    proc_ls = list(range(proc_st, proc_ed + 1))
                    uniq_ids = [str(cl_id) + '.' + str(pr_id) for pr_id in proc_ls]
                    if len(wu_ids) != len(uniq_ids):
                        content = "There is something wrong during submitting!"
                        self._logger.error(content)
                        return False, None
                    else:
                        comb = list(zip(wu_ids, uniq_ids))
                        out = dict(comb)
                        # remove job list after successful submission
                        os.remove(joblist)
                        return True, out
                except Exception as e:
                    self._logger.error(e, exc_info=True)
                    self._logger.error(outs)
                    return False, None
        return False, None

    def check_format(self, unique_id):
        '''Check the job status with fixed format
        @unique_id(int or str) The unique id of the job, e.g: cluster_id.proc_id
        @return(int or None) The job status'''

        args = ['-format', '%d\n', 'JobStatus']
        Id = str(unique_id)
        process = Popen(['condor_q', Id, *args], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            self._logger.info(stdout)
            self._logger.error(stderr)
            return None
        else:
            st = stdout.split()
            if len(st) == 1:
                try:
                    st = int(st[0])
                    return st
                except Exception as e:
                    self._logger.error(e, exc_info=True)
                    return None
            elif len(st) == 0:
                return 0
            else:
                content = "Unexpected output, job id %s isn't unique!"
                self._logger.error(content)
                self._logger.info(stdout)
                return None

    def check_running(self, studypath):
        '''Check the unfininshed job
        @studypath The absolute path of the study
        @return(list or None) The unique id (ClusterId.ProcId) list'''

        args = ['-constraint', 'regexp("%s", JobBatchName)' % studypath,
                '-constraint', 'JobStatus != 4', '-format', '%d.',
                'ClusterId', '-format', '%d\n', 'ProcId']
        process = Popen(['condor_q', *args], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            self._logger.info(stdout)
            self._logger.error(stderr)
            return None
        else:
            st = stdout.split()
            return st

    def check(self, *args, **kwargs):
        '''Check the job status'''
        for ky, vl in kwargs:
            args = args + ['-' + ky, vl]
        process = Popen(['condor_q', *args], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            self._logger.info(stdout)
            self._logger.error(stderr)
            return None
        else:
            self._logger.info(stdout)
            return stdout

    def remove(self, *args, **kwargs):
        '''Cancel the submitted jobs'''
        for ky, vl in kwargs:
            args = args + ['-' + ky, vl]
        process = Popen(['condor_rm', *args], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            self._logger.info(stdout)
            self._logger.error(stderr)
        else:
            self._logger.info(stdout)
            self._logger.error(stderr)
