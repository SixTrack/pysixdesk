import os
import shutil
import logging
import getpass

from abc import ABC, abstractmethod
from subprocess import Popen, PIPE

from . import utils


class Cluster(ABC):

    def __init__(self, temp_path):
        '''Constructor'''
        self._logger = logging.getLogger(__name__)

    @abstractmethod
    def prepare(self, task_ids, trans, exe, exe_args, input_path, output_path,
                *args, **kwargs):
        pass

    @abstractmethod
    def submit(self, input_path, job_name, trials=5, *args, **kwargs):
        pass

    @abstractmethod
    def check_running(self, *args, **kwargs):
        pass

    @abstractmethod
    def download_from_spool(self):
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

    def prepare(self, task_ids, trans, exe, exe_args, input_path, output_path,
                flavour='tomorrow', *args, **kwargs):
        '''Prepare the submission file.

        Args:
            task_ids (int): The task ids for submission
            trans (list): The python modules needed by the executables
            exe (str): The executable
            exe_args (str): The additional arguments for executable except for
            wu_id.
            input_path (str): The folder with input files
            output_path (str): The output folder
            flavour (str): The queue types of HTCondor
        '''
        job_list = os.path.join(input_path, 'job_id.list')
        if os.path.exists(job_list):
            os.remove(job_list)
        self._logger.info(f'Creating jobid.list file and output folder....')
        bar = utils.ProgressBar(len(task_ids))
        with open(job_list, 'w') as f_out:
            val_task_ids = []
            for i in task_ids:
                bar.update()
                if isinstance(i, list):
                    i = '-'.join(map(str, i))
                val_task_ids.append(i)
                out_f = os.path.join(output_path, str(i))
                if os.path.exists(out_f):
                    shutil.rmtree(out_f)
                os.makedirs(out_f)
            cont = '\n'.join(map(str, val_task_ids))
            f_out.write(cont)
        #os.chmod(job_list, 0o444)  # change the permission to readonly
        trans.append(os.path.join(utils.PYSIXDESK_ABSPATH, 'pysixdesk'))
        rep = {}
        rep['%func'] = ','.join(map(str, trans))
        rep['%exe'] = exe
        rep['%dirname'] = output_path
        rep['%joblist'] = job_list
        rep['%input'] = exe_args
        rep['%flavour'] = flavour
        if self.temp is None:
            self.temp = os.path.basename(input_path)
        sub_temp = os.path.join(self.temp, self.sub_name)
        sub_file = os.path.join(input_path, self.sub_name)
        if os.path.exists(sub_file):
            os.remove(sub_file)  # remove the old one
        self._logger.info(f'Creating actual htcondor description file....')
        with open(sub_temp, 'r') as f_in:
            with open(sub_file, 'w') as f_out:
                conts = f_in.read()
                for key, value in rep.items():
                    conts = conts.replace(key, value)
                f_out.write(conts)
        content = "The htcondor description file is ready!"
        self._logger.info(content)

    def submit(self, input_path, job_name, limit, trials=5, *args, **kwargs):
        '''Submit the job to the cluster
        Args:
            input_path (string): The input path to hold the input files
            job_name (string): The job name (also is the batch_name for HTCondor)
            limit (int): The maximum job number per submittion
            trials (int): The maximum number of resubmission when submit failed
        '''

        sub = os.path.join(input_path, self.sub_name)
        joblist = os.path.join(input_path, 'job_id.list')
        if not os.path.isfile(joblist):
            content = "There isn't %s job for submission!" % job_name
            self._logger.warning(content)
            return False, None
        with open(joblist, 'r') as f_in:
            task_ids = f_in.read().split()
        scont = 1
        while scont <= trials:
            self._logger.info(f'Submitting jobs to HTCondor....')
            args = list(args)
            for ky in kwargs.keys():
                args = args + ['-' + ky, kwargs[ky]]
            args.append(sub)
            args.append('-batch-name')
            args.append(job_name)
            valid_ids = list(task_ids)
            subed_ids = []
            try:
                while valid_ids:
                    with open(joblist, 'w') as f_out:
                        sub_ids = valid_ids[:limit]
                        valid_ids = list(set(valid_ids)-set(sub_ids))
                        out_str = '\n'.join(sub_ids)
                        f_out.write(out_str)
                    process = Popen(['condor_submit', '-terse', *args],
                                    stdout=PIPE, stderr=PIPE,
                                    universal_newlines=True)

                    stdout, stderr = process.communicate()
                    self._logger.info(stdout)
                    if stderr:
                        self._logger.error(stderr)
                    subed_ids.extend(sub_ids)
            except Exception as e:
                # this will catch the excpetion raised or any unexpected
                # exception in the try block.
                self._logger.error(e, exc_info=True)
            args = ['-constraint', 'JobBatchName=="%s"' % job_name,
                    '-format', '%d.', 'ClusterId', '-format', '%d\n', 'ProcId']
            uniq_ids = self.check(*args).split()
            status = len(subed_ids) == len(uniq_ids)
            if not status:
                content = "There is something wrong during submitting!"
                self._logger.error(content)
                scont += 1
            else:
                comb = list(zip(task_ids, uniq_ids))
                out = dict(comb)
                # remove job list after successful submission
                os.remove(joblist)
                return True, out
        return False, None

    def check_format(self, unique_id):
        '''Check the job status with fixed format
        Args:
            unique_id(int or str): The unique id of the job, e.g: cluster_id.proc_id
            return(int or None): The job status
        '''

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
        Args:
            studypath (string): The absolute path of the study
            return(list or None): The unique id (ClusterId.ProcId) list
        '''

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
        for ky in kwargs.keys():
            args = args + ['-' + ky, kwargs[ky]]
        process = Popen(['condor_q', *args], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stderr:
            self._logger.info(stdout)
            self._logger.error(stderr)
            return ''
        else:
            # self._logger.info(stdout)
            return stdout

    def download_from_spool(self, study_path, *args, **kwargs):
        '''Download the results from spool directory'''
        for ky in kwargs.keys():
            args = args + ['-' + ky, kwargs[ky]]
        user_name = getpass.getuser()
        theargs = [user_name, '-const', 'JobStatus==4', '-const',
                   'regexp("%s", JobBatchName)' % study_path] + list(args)
        process = Popen(['condor_transfer_data', *theargs], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        self._logger.info(stdout)
        if stderr:
            self._logger.error(stderr)
        return not process.returncode

    def remove(self,study_path, status, *args, **kwargs):
        '''Cancel the submitted jobs
        Args:
            studypath (string): The absolute path of the study
            status (int): The job status. 0: unexpanded, 1:idle, 2:held,
            3:removed, 4:done, 5:held, 6:submission_err
        '''
        if status not in [0, 1, 2, 3, 4, 5, 6]:
            self._logger.error("Unknown job status %s!" % status)
            return False
        for ky in kwargs.keys():
            args = args + ['-' + ky, kwargs[ky]]
        theargs = ['-const', 'JobStatus==%s' % status, '-const',
                   'regexp("%s", JobBatchName)' % study_path] + list(args)
        process = Popen(['condor_rm', *theargs], stdout=PIPE,
                        stderr=PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        if stdout:
            self._logger.info(stdout)
        if stderr:
            self._logger.error(stderr)
        return not process.returncode
