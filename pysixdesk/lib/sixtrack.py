#!/usr/bin/env python3
import os
import re
import time
import json
import shutil
import zipfile
import argparse
import configparser

from pathlib import Path
from contextlib import contextmanager

from pysixdesk.lib.pysixdb import SixDB
from pysixdesk.lib import utils
from pysixdesk.lib.dbtable import Table
from pysixdesk.lib.resultparser import parse_results


class TrackingJob:
    def __init__(self, task_id, input_info, group_name, logger):
        '''Class to handle the execution of the tracking job.

        Args:
            task_id (int): Current task ID.
            input_info (str/path): Path to the database configuration file.
            group_name (str): The group name when submitting multi-jobs to one node
            logger: The logger

        Raises:
            FileNotFoundError: If required input file is not found in database.
            ValueError: If unalbe to find the preprocess task_id for this job.
        '''
        self._logger = logger
        self._dest_path = Path('results', str(task_id))
        self._dest_path.mkdir(parents=True, exist_ok=True)
        self.group_name = group_name

        self.task_id = task_id
        # read database config
        cf = configparser.ConfigParser()
        cf.optionxform = str
        cf.read(input_info)
        self.cf = cf
        self.db = SixDB(cf['db_info'].items())
        db_type = cf['db_info']['db_type']
        self.db_type = db_type.lower()

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
        self.six_cfg = cf['sixtrack']
        self._decomp_templates()
        self.boinc_cfg = cf['boinc']

        self.six_out = json.loads(self.six_cfg['output_files'])

        pre_task_id = self.db.select('preprocess_wu',
                                     ['task_id'],
                                     f'wu_id={self.preprocess_id}')
        if not pre_task_id:
            raise ValueError("Can't find the preprocess task_id for this job!")
        self.pre_task_id = pre_task_id[0][0]

        self.cr_files = ['crpoint_sec.bin', 'crpoint_pri.bin',
                         'fort.6', 'singletrackfile.dat']

        self.cr_inputs = self._decomp_files()

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
        '''Convenience function to convert string to bool.

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

    def _decomp_templates(self):
        """Decompresses the template buffers from the database.

        Raises:
            FileNotFoundError: If buffer is not found in db.
        """
        templates = self.cf['templates']
        temp_buf = self.db.select('templates', templates.keys())[0]
        if not temp_buf:
            raise FileNotFoundError('Templates not found in DB.')
        else:
            for temp, temp_name in zip(temp_buf, templates.values()):
                if not temp:
                    raise FileNotFoundError(f'{temp_name} not found in DB.')
                else:
                    utils.decompress_buf(temp, temp_name)

    def _decomp_files(self):
        '''This decompresses the buffers in the database into files.
        Note: the db connection must be open.

        Raises:
            FileNotFoundError: If unable to find input buffers in database.
            Or unable to find checkpoint buffers.
        '''

        inp = self.six_cfg["input_files"]
        input_files = json.loads(inp)
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

    @contextmanager
    def sixtrack_temp_folder(self, folder='temp', symlink_parent=True,
                             extra=[]):
        """Helper context manager to deal with the temp folder and symlinking
        the input files. On release, moves back to the orignal folder and
        deletes the temp folder.

        Args:
            folder (str, optional): name of temp folder.
            symlink_parent (bool, optional): controls whether to symlink input
            files and extra files from the parent dir to the temporary folder.
            extra (list, optional): list of extra files to symlink.

        Raises:
            FileNotFoundError: if input file not found during symlinking.
        """
        # quick context manager to handle the temporary folder
        cwd = Path.cwd()
        temp_folder = Path(folder)
        if temp_folder.is_dir():
            shutil.rmtree(temp_folder, ignore_errors=True)
        temp_folder.mkdir()
        try:
            os.chdir(temp_folder)
            # symlink input files to temp folder.
            if symlink_parent:
                input_files = json.loads(self.six_cfg["input_files"])
                # make symlinks to the other input files in the parent folder
                for file in [Path(f) for f in list(input_files.values()) + extra]:
                    target = Path.cwd().parent / file
                    if target.is_file():
                        file.symlink_to(target)
                    else:
                        msg = f"The required input file {file} was not found!"
                        raise FileNotFoundError(msg)
            yield
        finally:
            os.chdir(cwd)
            shutil.rmtree(temp_folder, ignore_errors=True)

    def sixtrack_prep_cfg(self, **kwargs):
        """Prepares sixtrack's fort.3 config, by adding the length of the
        machine, read in the fort.3.aux file, and any extra kwargs.

        Args:
            **kwargs: are added to the config.

        Returns:
            dict: fort.3 placeholder dictionnary with the added keys/values.
        """
        # make a dict copy of fort_cfg
        fort_dic = dict(self.fort_cfg.items())
        # reads the length from fort.3.aux
        with open('fort.3.aux', 'r') as fc3aux:
            fc3aux_lines = fc3aux.readlines()
        fc3aux_2 = fc3aux_lines[1]
        c = fc3aux_2.split()
        lhc_length = c[4]
        fort_dic['length'] = lhc_length
        # adds additional kwargs to fort_dic
        fort_dic.update(kwargs)
        return fort_dic

    def sixtrack_prep_job(self, fort_cfg, source_prefix=None,
                          output_file='fort.3'):
        """Prepares sixtrack fort.3 file.

        Args:
            fort_cfg (dict): dict containing the placeholder/value pairs.
            source_prefix (str/path, optional): if provided, will use the
            provided folder prefix when looking for the fort_file and fc.3
            files.
            output_file (str, optional): name of the prepared fort.3 file.

        """
        # get the fort file patterns and values
        if source_prefix is not None:
            source_prefix = Path(source_prefix)
        # touch fort.6
        open('fort.6', 'a').close()

        fort_file = Path(self.six_cfg["fort_file"])
        patterns = ['%' + a for a in fort_cfg.keys()]
        values = list(fort_cfg.values())
        dest = fort_file.with_suffix('.temp')

        source = fort_file
        if source_prefix is not None:
            source = source_prefix / source

        utils.replace(patterns, values, source, dest)

        # prepare the other input files
        input_files = json.loads(self.six_cfg["input_files"])

        madx_fc3 = input_files['fc.3']
        if source_prefix is not None:
            madx_fc3 = source_prefix / madx_fc3

        # concatenate
        utils.concatenate_files([dest, madx_fc3], output_file)

    def sixtrack_run(self, output_file):
        """Runs sixtrack.

        Args:
            output_file (str): file in which to write sixtrack's stdout.
        """
        # actually run
        six_output = os.popen(self.six_cfg["sixtrack_exe"])
        self._logger.info('Sixtrack is running...')
        # write stdout to file
        outputlines = six_output.readlines()
        self._logger.info('Sixtrack is done!')

        if outputlines:
            with open(output_file, 'w') as six_out:
                six_out.writelines(outputlines)
        elif output_file != 'fort.6':
            # For some sixtrack version, the stdout will be automatically
            # written to fort.6
            shutil.copy2('fort.6', output_file)

    def dl_output(self):
        """Downloads the output of the job.
        """
        # Download the requested files.
        down_list = list(self.six_out)
        if self.boinc:
            down_list = ['fort.3']
        else:
            down_list.append('fort.3')
        for cr_f in self.cr_files:
            if Path(cr_f).exists():
                down_list.append(cr_f)

        if self.db_type == 'mysql':
            down_list.extend(['_condor_stdout', '_condor_stderr'])

        try:
            utils.download_output(down_list, self._dest_path)
            content = f"All requested results have been stored in {self._dest_path}"
            self._logger.info(content)
        except Exception:
            self._logger.warning("Job failed!", exc_info=True)

    def push_to_db(self):
        '''Runs the parsing and pushes results to db.
        '''
        self.db.open()

        task_table = {}
        task_table['status'] = 'Success'

        result_cf = {}
        for sec in self.cf:
            result_cf[sec] = dict(self.cf[sec])
        filelist = Table.result_table(self.six_out)
        parse_results('sixtrack', self.task_id, self._dest_path, filelist,
                      task_table, result_cf)

        self.db.update('sixtrack_task', task_table,
                       f'task_id={self.task_id}')

        for sec, val in result_cf.items():
            val['task_id'] = [self.task_id] * len(val['mtime'])
            self.db.insertm(sec, val)

        job_table = {}
        if task_table['status'] == 'Success':
            job_table['status'] = 'complete'
            job_table['mtime'] = int(time.time() * 1E7)
            content = f" sixtrack task {self.task_id} has completed normally!"
            self._logger.info(content)
        else:
            job_table['status'] = 'incomplete'
            job_table['mtime'] = int(time.time() * 1E7)
            self._logger.warning("This is a failed job!")

        self.db.update(f'sixtrack_wu', job_table, f'task_id={self.task_id}')
        shutil.rmtree(self._dest_path)

    def run(self):
        '''Main execution logic
        '''
        try:
            self.sixtrack_job()
        except Exception:
            self._logger.error('Sixtrack task failed!', exc_info=True)

        self.dl_output()

        if self.db_type == 'mysql':
            self.push_to_db()

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
        job_name = st_pre + '__' + self.job_name + '_task_id_' +\
                str(self.task_id) + '_group_' + str(self.group_name)
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
            FileNotFoundError: If missing files for boinc job.
        """
        # zip all the input files, e.g. fort.3 fort.2 fort.8 fort.16
        input_zip = job_name + '.zip'
        inputs = ['fort.2', 'fort.3', 'fort.8', 'fort.16'] + self.cr_inputs
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
        ''''Controls sixtrack job execution.
        '''
        if 'additional_input' in self.six_cfg.keys():
            # inp = self.six_cfg["additional_input"]
            add_inputs = json.loads(self.six_cfg["additional_input"])
        else:
            add_inputs = []

        if self.boinc:
            # run sixtrack with a few turns
            fort_dic = self.sixtrack_prep_cfg(turnss=self.six_cfg['test_turn'])
        else:
            # run complete sixtrack job
            fort_dic = self.sixtrack_prep_cfg()

        # create and enter temp folder
        with self.sixtrack_temp_folder(symlink_parent=True,
                                       extra=add_inputs + self.cr_inputs):
            self._logger.info("Preparing the sixtrack input files!")
            # replace placeholders and concatenate
            self.sixtrack_prep_job(fort_dic,
                                   source_prefix=Path.cwd().parent,
                                   output_file='fort.3')
            # run sixtrack
            self.sixtrack_run('fort.6')
            # self.six_out.append('fort.6')

            # check and move output files
            if utils.check(self.six_out):
                for out in self.six_out:
                    shutil.copy2(out, Path.cwd().parent / out)

            # move checkpoint files out of temp folder, if files are not
            # symlinks
            for cr_f in self.cr_files:
                cr_f = Path(cr_f)
                cr_f_parent = Path.cwd().parent / cr_f
                if cr_f.exists() and not cr_f_parent.exists():
                    shutil.copy2(cr_f, cr_f_parent)

            if not self.boinc:
                shutil.copy2(Path('fort.3'), Path.cwd().parent / 'fort.3')
        # leave and delete temp folder

        if self.boinc:
            if not self.sixtrack_check_tracking(six_stdout='fort.6'):
                raise Exception(f"The job {self.task_id} doesn't pass the test!")

            job_name = self.boinc_prep()
            # restore the desired number of turns
            fort_dic['turnss'] = self.fort_cfg['turnss']
            self.sixtrack_prep_job(fort_dic,
                                   source_prefix=None,
                                   output_file='fort.3')
            self.boinc_submit(job_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('task_id', type=str,
                        help='Current work unit ID')
    parser.add_argument('input_info', type=str,
                        help='Path to the db config file.')
    args = parser.parse_args()
    group_name = args.task_id
    task_ids = group_name.split('-')
    LOGGER = utils.condor_logger('sixtrack')
    for task_id in task_ids:
        job = TrackingJob(task_id, args.input_info, group_name, LOGGER)
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
            raise e
        finally:
            if job.db_type == 'mysql':
                job.db.remove('sixtrack_wu_tmp', where=f'task_id={job.task_id}')
