#!/usr/bin/env python3
import os
import re
import shutil
import zipfile
import argparse
import configparser
import time

from pathlib import Path

from pysixdesk.lib.sixtrack_job import SixtrackJob
from pysixdesk.lib.pysixdb import SixDB
from pysixdesk.lib import utils
from pysixdesk.lib.dbtable import Table
from pysixdesk.lib.resultparser import parse_results


class TrackingJob(SixtrackJob):
    def __init__(self, task_id, db_name):
        '''Class to handle the execution of the tracking job.

        Args:
            wu_id (int): Current work unit ID.
            db_name (str/path): Path to the database configuration file.

        Raises:
            FileNotFoundError: If required input file is not found in database.
            Exception: If unalbe to find the preprocess task_id for this job.
        '''
        self._logger = utils.condor_logger('sixtrack')
        self.task_id = task_id
        # read database config
        cf = configparser.ConfigParser()
        cf.optionxform = str
        cf.read(db_name)
        self.db = SixDB(cf['db_info'].items())
        db_type = cf['db_info']['db_type']
        self.db_type = db_type.lower()
        # cf.clear()
        # get some parameters from the database
        outputs = self.db.select('sixtrack_wu_tmp',
                                 ['preprocess_id', 'boinc', 'job_name',
                                  'wu_id', 'first_turn'],
                                 where=f'task_id={self.task_id}')
        if not outputs[0]:
            content = "Data not found for sixtrack task %s!" % self.task_id
            raise FileNotFoundError(content)

        self.preprocess_id = outputs[0][0]
        self.boinc = self._str_to_bool(outputs[0][1])
        self.job_name = outputs[0][2]
        self.wu_id = outputs[0][3]
        self.first_turn = outputs[0][4]

        fort3_keys = list(cf['fort3'].keys())
        fort3_outputs = self.db.select("sixtrack_wu_tmp", fort3_keys,
                                       where=f"task_id={self.task_id}")
        self.fort_cfg = dict(zip(fort3_keys, fort3_outputs[0]))
        # in_files = utils.decompress_buf(input_buf, None, 'buf')
        # cf.read_string(in_files)
        self.cf = cf
        self.six_cfg = cf['sixtrack']
        # self.fort_cfg = cf['fort3']
        self.boinc_cfg = cf['boinc']

        # inp = self.six_cfg['input_file']
        # input_files = utils.decode_strings(inp)
        # self.six_inp = list(input_files.values())

        out = self.six_cfg['output_files']
        output_files = utils.decode_strings(out)
        self.six_out = output_files

        pre_task_id = self.db.select('preprocess_wu',
                                     ['task_id'],
                                     f'wu_id={self.preprocess_id}')
        if not pre_task_id:
            raise Exception("Can't find the preprocess task_id for this job!")
        self.pre_task_id = pre_task_id[0][0]

        self.cr_inputs = self._decomp_files()

        self.cr_files = ['crpoint_sec.bin', 'crpoint_pri.bin',
                         'fort.6', 'singletrackfile.dat']
        # get boinc settings
        boinc_infos = self.db.select('env',
                                     ['boinc_work', 'boinc_results',
                                      'surv_percent'])
        if not boinc_infos:
            self.boinc = False
            self.boinc_work = None
            self.boinc_results = None
            self.surv_percent = 1
            self._logger.error("There isn't a valid boinc path to submit this task!")
        else:
            self.boinc_work = Path(boinc_infos[0][0])
            self.boinc_results = Path(boinc_infos[0][1])
            self.surv_percent = boinc_infos[0][2]

        # close db to avoid unexpected timeouts
        self.db.close()

    def _str_to_bool(self, string):
        '''Conveniance function to convert string to bool.

        Args:
            string (str): String to convert to bool.

        Returns:
            bool: Converted bool, True if string in ['true', 'yes', 'on'] and
            False is string in ['false', 'no', 'off'].

        Raises:
            ValueError: If string not recognised.
        '''

        string_l = string.lower()
        if string_l in ['true', 'yes', 'on']:
            return True
        elif string_l in ['false', 'no', 'off']:
            return False
        else:
            raise ValueError(f'Unable to convert "{string}" to bool.')

    def _decomp_files(self):
        '''This decompresses the buffers in the database into files.
        Note: the db connection must be open.

        Raises:
            FileNotFoundError: If unble to find input buffers in database.
        '''

        inp = self.six_cfg["input_files"]
        input_files = utils.decode_strings(inp)
        inputs = list(input_files.values())

        input_buf = self.db.select('preprocess_task',
                                   inputs,
                                   f'task_id={self.pre_task_id}')
        if not input_buf:
            raise FileNotFoundError("The required files were not found!")

        input_buf = list(input_buf[0])

        cr_inputs = []
        if self.first_turn is not None:
            cr_inputs = self.cr_files
            where = f'wu_id={self.wu_id} and last_turn={self.first_turn-1}'
            cr_task_ids = self.db.select('sixtrack_wu', ['task_id'],
                                         where=where)
            cr_task_id = cr_task_ids[0][0]
            cr_input_buf = self.db.select('sixtrack_task', cr_inputs,
                                          where=f'task_id={cr_task_id}')
            if (not cr_input_buf) or (cr_input_buf[0][0] is None):
                raise FileNotFoundError("checkpoint files were not found!")

            inputs += cr_inputs
            input_buf += cr_input_buf[0]

        for infile in inputs:
            i = inputs.index(infile)
            buf = input_buf[i]

            utils.decompress_buf(buf, infile, des='file')

        return cr_inputs

    def dl_output(self, dest_path):
        """Downloads the output of the job.

        Args:
            dest_path (str): Path to download location.
        """
        # Download the requested files.
        down_list = list(self.six_out)
        down_list.append('fort.3')
        for cr_f in self.cr_files:
            if Path(cr_f).exists():
                down_list.append(cr_f)

        try:
            utils.download_output(down_list, dest_path)
            content = f"All requested results have been stored in {dest_path}"
            self._logger.info(content)
            if self.boinc:
                down_list = ['fort.3']
                dest_path = Path(self.six_cfg["dest_path"]) / str(self.task_id)
                utils.download_output(down_list, dest_path)
        except Exception:
            self._logger.warning("Job failed!", exc_info=True)

    def push_to_db(self, dest_path):
        '''
        Runs the parsing and pushes results to db.

        Args:
            dest_path (str): Location of the job results.
        '''
        self.db.open()

        task_table = {}
        task_table['status'] = 'Success'

        result_cf = {}
        for sec in self.cf:
            result_cf[sec] = dict(self.cf[sec])
        filelist = Table.result_table(self.six_out)
        parse_results('sixtrack', self.task_id, dest_path, filelist,
                      task_table, result_cf)

        self.db.update('sixtrack_task', task_table,
                       f'task_id={self.task_id}')

        for sec, val in result_cf.items():
            val['task_id'] = [self.task_id] * len(val['mtime'])
            self.db.insertm(sec, val)

        self.push_to_db_status(task_table, job_type='sixtrack')

    def run(self):
        '''
        Main execution logic
        '''
        try:
            self.sixtrack_job()
        except Exception:
            self._logger.error('Sixtrack task failed!', exc_info=True)

        if self.db_type == 'mysql':
            dest_path = Path('./result')
        else:
            dest_path = Path(self.six_cfg["dest_path"]) / str(self.task_id)
        if not dest_path.is_dir():
            dest_path.mkdir(parents=True, exist_ok=True)

        self.dl_output(dest_path)

        if self.db_type == 'mysql':
            self.push_to_db(dest_path)

    def sixtrack_check_tracking(self, six_stdout='sixtrack.output'):
        '''Check the tracking result to see how many particles survived.

        Args:
            six_stdout (str, optional): file containing sixtrack's standard
            output.

        Returns:
            bool: True if the ratio of particles which survived the tracking is
            >= than self.surv_precent. False otherwise.
        '''
        with open(six_stdout, 'r') as f_in:
            lines = f_in.readlines()
        try:
            track_lines = filter(lambda x: re.search(r'TRACKING>', x), lines)
            last_line = list(track_lines)[-1]
            info = re.split(r':|,', last_line)
            turn_info = info[1].split()
            part_info = info[-1].split()
            total_turn = float(turn_info[-1])
            track_turn = float(turn_info[1])
            total_part = float(part_info[-1])
            surv_part = float(part_info[0])
            surv_ratio = surv_part / total_part
            return track_turn >= total_turn and surv_ratio >= self.surv_percent

        except Exception as e:
            self._logger.error(e)
            return False

    def boinc_prep(self):
        """Prepares the files needed to boinc submittion.

        Returns:
            str: the boinc job name.
        """
        st_pre = self.boinc_work.parent.name
        job_name = st_pre + '__' + self.job_name + '_task_id_' + str(self.task_id)
        if not self.boinc_work.is_dir():
            self.boinc_work.mkdir(parents=True, exist_ok=True)
        if not self.boinc_results.is_dir():
            self.boinc_results.mkdir(parents=True, exist_ok=True)
        content = 'The job passes the test and will be sumbitted to BOINC!'
        self._logger.info(content)
        return job_name

    def boinc_submit(self, job_name):
        """Submit to boinc.

        Args:
            job_name (str): boinc job name.

        Raises:
            Exception: If failure to copy files to boinc directory.
            FileNotFoundError: If missing files for boinc job.
        """
        # zip all the input files, e.g. fort.3 fort.2 fort.8 fort.16
        input_zip = job_name + '.zip'
        inputs = ['fort.2', 'fort.3', 'fort.8', 'fort.16']
        with zipfile.ZipFile(input_zip, 'w', zipfile.ZIP_DEFLATED) as ziph:
            for infile in inputs:
                if infile in os.listdir('.'):
                    ziph.write(infile)
                else:
                    content = "The required file %s isn't found!" % infile
                    raise FileNotFoundError(content)

        boinc_cfg_file = job_name + '.desc'
        self.boinc_cfg['workunitName'] = job_name
        with open(boinc_cfg_file, 'w') as f_out:
            pars = '\n'.join(self.boinc_cfg.values())
            f_out.write(pars)
            f_out.write('\n')
        for f in [input_zip, boinc_cfg_file]:
            shutil.copy2(f, self.boinc_work)
        self._logger.info("Submit to %s successfully!" % self.boinc_work)

    def sixtrack_job(self):
        ''''
        Controls sixtrack job execution.
        '''
        if 'additional_input' in self.six_cfg.keys():
            inp = self.six_cfg["additional_input"]
            add_inputs = utils.decode_strings(inp)
        else:
            add_inputs = []

        self.sixtrack_copy_input(extra=add_inputs + self.cr_inputs)

        if self.boinc:
            # run sixtrack with a few turns
            fort_dic = self.sixtrack_prep_cfg(turnss=self.six_cfg['test_turn'])
        else:
            # run complete sixtrack job
            fort_dic = self.sixtrack_prep_cfg()

        # create and enter temp folder
        with self.sixtrack_temp_folder():
            self._logger.info("Preparing the sixtrack input files!")
            # replace placeholders, concatenate and prepare symlinks
            self.sixtrack_prep_job(fort_dic,
                                   source_prefix=Path.cwd().parent,
                                   symlink_parent=True,
                                   output_file='fort.3')
            # run sixtrack
            self.sixtrack_run('fort.6')
            self.six_out.append('fort.6')
            # check and move output files
            if utils.check(self.six_out):
                for out in self.six_out:
                    shutil.copy2(out, Path.cwd().parent / out)

            # move checkpoint files out of temp folder
            for cr_f in self.cr_files:
                if Path(cr_f).exists():
                    shutil.copy2(cr_f, Path.cwd().parent / cr_f)

            if not self.boinc:
                shutil.copy2(Path('fort.3'), Path.cwd().parent / 'fort.3')
        # leave and delete temp folder

        if self.boinc:
            if not self.sixtrack_check_tracking(six_stdout='fort.6'):
                raise Exception("The job doesn't pass the test!")

            job_name = self.boinc_prep()
            # restore the desired number of turns
            fort_dic['turnss'] = self.fort_cfg['turnss']
            self.sixtrack_prep_job(fort_dic,
                                   source_prefix=None,
                                   output_file='fort.3',
                                   symlink_parent=False)
            self.boinc_submit(job_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('task_id', type=int,
                        help='Current work unit ID')
    parser.add_argument('input_info', type=str,
                        help='Path to the db config file.')
    args = parser.parse_args()
    job = TrackingJob(args.task_id, args.input_info)
    try:
        job.run()
    except Exception as e:
        if job.db_type == 'mysql':
            job.db.open()
            job_table = {}
            where = "task_id"
            job_table['status'] = 'incomplete'
            job_table['mtime'] = int(time.time() * 1E7)
            job.db.update('sixtrack_wu', job_table,
                          where=f'task_id={job.task_id}')
            # this error msg is not correct.
            # logger.error("Error during reconnection.")
        raise e
    finally:
        if job.db_type == 'mysql':
            job.db.remove('sixtrack_wu_tmp', where=f'task_id={job.task_id}')
