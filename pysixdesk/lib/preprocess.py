#!/usr/bin/env python3
import os
import shutil
import configparser
import argparse
import time

from pathlib import Path
from contextlib import contextmanager

from pysixdesk.lib import utils
from pysixdesk.lib.dbtable import Table
from pysixdesk.lib import generate_fort2
from pysixdesk.lib.pysixdb import SixDB
from pysixdesk.lib.resultparser import parse_results


class PreprocessJob:
    def __init__(self, task_id, input_info):
        '''Class to handle the execution of the preprocessing job.

        Args:
            task_id (int): Current work unit ID.
            input_info (str/path): Path to the database configuration file.

        Raises:
            FileNotFoundError: If required input file is not found in database.
            ValueError: If the provided output files are incorect.
        '''
        self._logger = utils.condor_logger('preprocess')
        self.task_id = task_id
        cf = configparser.ConfigParser()
        cf.optionxform = str
        cf.read(input_info)
        self.db = SixDB(cf['db_info'].items())
        db_type = cf['db_info']['db_type']
        self.db_type = db_type.lower()
        # cf.clear()

        mask_keys = list(cf['mask'].keys())
        outputs = self.db.select('preprocess_wu', mask_keys,
                                 where=f'task_id={self.task_id}')

        if not outputs[0]:
            content = "Data not found for preprocess task %s!" % task_id
            raise FileNotFoundError(content)

        self.madx_cfg = cf['madx']
        self.mask_cfg = dict(zip(mask_keys, outputs[0]))
        # outputs = self.db.select('preprocess_wu',
        #                          ['input_file', 'task_id'],
        #                          f'task_id={task_id}')

        # self.task_id = outputs[0][1]
        # intput_buf = outputs[0][0]
        # in_files = utils.decompress_buf(intput_buf, None, 'buf')
        # cf.read_string(in_files)
        self.cf = cf
        # self.madx_cfg = cf['madx']

        output_files = self.madx_cfg["output_files"]
        output_files = utils.decode_strings(output_files)
        self.madx_out = output_files
        # self.mask_cfg = cf['mask']

        self.oneturn_flag = self.madx_cfg.getboolean('oneturn')
        self.coll_flag = self.madx_cfg.getboolean('collimation')

        if self.oneturn_flag:
            self.six_cfg = cf['sixtrack']
            self.fort_cfg = cf['fort3']
        else:
            self.six_cfg = {}
            self.fort_cfg = {}
        if self.coll_flag:
            self.coll_cfg = cf['collimation']
        else:
            self.coll_cfg = {}
        # close db to avoid unexpected timeouts
        self.db.close()

    @contextmanager
    def sixtrack_temp_folder(self, folder='temp'):
        """Helper context manager to deal with the temp folder. On release,
        moves back to the orignal folder and deletes the temp folder.

        Args:
            folder (str, optional): name of temp folder.
        """
        # quick context manager to handle the temporary folder
        cwd = Path.cwd()
        temp_folder = Path(folder)
        if temp_folder.is_dir():
            shutil.rmtree(temp_folder, ignore_errors=True)
        temp_folder.mkdir()
        try:
            os.chdir(temp_folder)
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
                          output_file='fort.3', symlink_parent=True):
        """Prepares sixtrack fort.3 file and symlinks the other required
        input files.

        Args:
            fort_cfg (dict): dict containing the placeholder/value pairs.
            source_prefix (str/path, optional): if provided, will use the
            provided folder prefix when looking for the fort_file and fc.3
            files.
            output_file (str, optional): name of the prepared fort.3 file.
            symlink_parent (bool, optional): controls whether to symlink the
            input_files of the parent folder to the current folder.

        Raises:
            Exception: If placeholder replacement in fort.3 fails.
            FileNotFoundError: If missing input file during symlinking.
            ValueError: If provided sixtrack 'input_files' are incorect.
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
        input_files = utils.decode_strings(self.six_cfg["input_files"])

        madx_fc3 = input_files['fc.3']
        if source_prefix is not None:
            madx_fc3 = source_prefix / madx_fc3

        # concatenate
        utils.concatenate_files([dest, madx_fc3], output_file)
        # if not source.samefile(output_file):
        #     utils.diff(source, output_file, logger=self._logger)

        # there could be merit in moving the symlinking to sixtrack_temp_folder
        # so it would be done when entering a temp folder
        if symlink_parent:
            # make symlinks to the other input files in the parent folder
            for file in [Path(f) for f in input_files.values()]:
                target = Path.cwd().parent / file
                if target.is_file():
                    file.symlink_to(target)
                else:
                    raise FileNotFoundError("The required input file %s does not found!" %
                                            file)

    def sixtrack_copy_input(self):
        '''
        Copies the fort.3 file and other additional inputs to the cwd.

        Args:
            extra (list, optional): Any additional inputs to copy over.
        '''
        required = [self.six_cfg['fort_file']]
        for infile in required:
            infi = Path(self.six_cfg['source_path']) / infile
            if infi.is_file():
                shutil.copy2(infi, infile)
            else:
                content = "The required file %s isn't found!" % infile
                raise FileNotFoundError(content)

    def sixtrack_run(self, output_file):
        """Runs sixtrack.

        Args:
            output_file (str): File in which to write sixtrack's stdout.
        """
        # actually run
        six_output = os.popen(self.six_cfg["sixtrack_exe"])
        self._logger.info('Sixtrack is running...')
        # write stdout to file
        outputlines = six_output.readlines()
        self._logger.info('Sixtrack is done!')
        # output_name = Path.cwd().parent / (job_name + '.output')

        if outputlines and output_file is not None:
            with open(output_file, 'w') as six_out:
                six_out.writelines(outputlines)
        elif output_file != 'fort.6':
            # For some sixtrack version, the stdout will be automatically
            # written to fort.6
            shutil.copy2('fort.6', output_file)
        # else:
        #     self.output_files.append('fort.6')

        # print(''.join(outputlines))

    def dl_output(self, dest_path):
        """Downloads the output of the job.

        Args:
            dest_path (str): Path to download location.
        """
        # Download the requested files.
        madx_outputs = list(self.madx_out.values())
        madx_outputs.append('madx_in')
        madx_outputs.append('madx_stdout')
        if self.oneturn_flag:
            self.madx_out['oneturnresult'] = 'oneturnresult'
            madx_outputs.append('oneturnresult')
        if self.coll_flag:
            madx_outputs.append('fort3.limi')
        try:
            utils.download_output(madx_outputs, dest_path)
            content = f"All requested results have been stored in {dest_path}"
            self._logger.info(content)
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
        filelist = Table.result_table(self.madx_out.values())
        parse_results('preprocess', self.task_id, dest_path, filelist,
                      task_table, result_cf)

        self.db.update(f'preprocess_task', task_table,
                       f'task_id={self.task_id}')

        for sec, val in result_cf.items():
            val['task_id'] = [self.task_id] * len(val['mtime'])
            self.db.insertm(sec, val)

        job_table = {}
        if task_table['status'] == 'Success':
            job_table['status'] = 'complete'
            job_table['mtime'] = int(time.time() * 1E7)
            self.db.update(f'preprocess_wu', job_table, f'task_id={self.task_id}')
            content = f" preprocess task {self.task_id} has completed normally!"
            self._logger.info(content)
        else:
            job_table['status'] = 'incomplete'
            job_table['mtime'] = int(time.time() * 1E7)
            self.db.update(f'preprocess_wu', job_table, f'task_id={self.task_id}')
            self._logger.warning("This is a failed job!")

    def run(self):
        '''
        Main execution logic.
        '''
        try:
            self.madx_job()
        except Exception as e:
            if self.db_type == 'sql':
                # madx failed and sql --> stop here
                content = 'MADX job failed.'
                self._logger.error(content)
                raise e
            else:
                # madx failed and mysql --> store in database
                content = 'MADX job failed.'
                self._logger.error(content, exc_info=True)
        else:
            # madx did not fail --> run sixtrack
            if self.coll_flag:
                try:
                    self.new_fort2()
                except Exception:
                    self._logger.error('Generation of new fort2 failed!',
                                       exc_info=True)
            if self.oneturn_flag:
                try:
                    self.sixtrack_job()
                    self.write_oneturnresult()
                except Exception:
                    self._logger.error('Oneturn job failed!', exc_info=True)
        finally:
            # if failed and mysql or if success --> store results
            # result store location
            if self.db_type == 'mysql':
                dest_path = Path('./result')
            else:
                dest_path = Path(self.madx_cfg["dest_path"]) / str(self.task_id)
            if not dest_path.is_dir():
                dest_path.mkdir(parents=True, exist_ok=True)
            # download results
            self.dl_output(dest_path)
            # push results to mysql db
            if self.db_type == 'mysql':
                if self.coll_flag:
                    self.madx_out['fort3.limi'] = 'fort3.limi'
                self.push_to_db(dest_path)

    def madx_copy_mask(self):
        '''
        Copies madx mask file to cwd.
        '''
        mask_name = self.madx_cfg["mask_file"]
        source_path = Path(self.madx_cfg['source_path'])
        shutil.copy2(source_path / mask_name, mask_name)
        # make destination folder
        Path(self.madx_cfg['dest_path']).mkdir(parents=True, exist_ok=True)

    def madx_prep(self, output_file='madx_in'):
        '''Replaces the placeholders in the mask_file and prints diff.

        Args:
            output_file (str, optional): Name of the prepared mask_file.

        Raises:
            Exception: if failure to generate mask file.
        '''
        patterns = ['%' + a for a in self.mask_cfg.keys()]
        values = list(self.mask_cfg.values())
        utils.replace(patterns, values, self.madx_cfg["mask_file"],
                      output_file)
        # show diff
        # mask_name = self.madx_cfg["mask_file"]
        # utils.diff(mask_name, output_file, logger=self._logger)

    def madx_run(self, mask):
        """Runs madx.

        Args:
            mask (str): mask file on which to run madx.

        Raises:
            Exception: If 'finished normally' is not in Madx output.
        """
        exe = self.madx_cfg['madx_exe']
        command = exe + " " + mask
        self._logger.info("Calling madx %s" % exe)
        self._logger.info("MADX job is running...")
        output = os.popen(command)
        output = output.readlines()
        with open('madx_stdout', 'w') as mad_out:
            mad_out.writelines(output)
        if 'finished normally' not in output[-2]:
            content = "MADX has not completed properly!"
            raise Exception(content)
        else:
            self._logger.info("MADX has completed properly!")

    def madx_job(self):
        """
        Controls madx job execution.
        """
        self.madx_copy_mask()
        # replace placeholders
        ready_mask = 'madx_in'
        self.madx_prep(output_file=ready_mask)
        # run job
        self.madx_run(ready_mask)
        # check the output
        if not utils.check(self.madx_out):
            content = 'MADX output files not found.'
            raise FileNotFoundError(content)

    def new_fort2(self):
        '''
        Generate new fort.2 with aperture markers and survey and fort3.limit.
        '''
        # copy the collimation input files over
        inp = self.coll_cfg['input_files']
        inputfiles = utils.decode_strings(inp)
        source_path = Path(self.coll_cfg["source_path"])
        for fil in inputfiles.values():
            fl = source_path / fil
            shutil.copy2(fl, fil)
        fc2 = 'fort.2'
        aperture = inputfiles['aperture']
        survery = inputfiles['survey']
        # generate fort2
        generate_fort2.run(fc2, aperture, survery)

    def sixtrack_check(self, job_name):
        """Checks for fort.10 and moves it out of temp folder.
        fort.10 --> ../fort.10_job_name

        Args:
            job_name (str): name of the sixtrack job.

        Raises:
            FileNotFoundError: if fort.10 is not found.
        """
        # check for fort.10
        # output_name = Path.cwd().parent / (job_name + '.output')
        # TODO: it is possible for sixtrack to run correctly but there not be a
        # fort.10
        if not Path('fort.10').is_file:
            self._logger.error("The %s sixtrack job FAILED!" % job_name)
            self._logger.error("Check the file %s which contains the SixTrack fort.6 output." % job_name)
            raise FileNotFoundError('"fort.10" not found.')
        else:
            # move fort.10 out of temp folder
            result_name = Path.cwd().parent / ('fort.10' + '_' + job_name)
            shutil.move('fort.10', result_name)
            self._logger.info('Sixtrack job %s has completed normally!' % job_name)

    def _sixtrack_job(self, job_name, **kwargs):
        '''One turn sixtrack job.

        Args:
            job_name (str): name of the sixtrack job.
            **kwargs: forwarded to sixtrack_prep_cfg, used as key value pairs
            in fort_cfg.
        '''
        fort_dic = self.sixtrack_prep_cfg(**kwargs)
        with self.sixtrack_temp_folder():
            self.sixtrack_prep_job(fort_dic,
                                   source_prefix=Path.cwd().parent,
                                   symlink_parent=True,
                                   output_file='fort.3')
            self.sixtrack_run(job_name)
            # check and move fort.10 file
            self.sixtrack_check(job_name)

    def sixtrack_job(self):
        ''''
        Controls sixtrack job execution.
        '''
        self.sixtrack_copy_input()

        try:
            self._sixtrack_job('first_oneturn', dp1='.0', dp2='.0', ition='0')
        except Exception as e:
            self._logger.error('SixTrack first oneturn failed.')
            raise e

        try:
            self._sixtrack_job('second_oneturn', ition='0')
        except Exception as e:
            self._logger.error('SixTrack second oneturn failed.')
            raise e

        # try:
        #     self._sixtrack_job('beta_oneturn', dp1='.0', dp2='.0')
        # except Exception as e:
        #     self._logger.error('SixTrack beta oneturn failed.')
        #     raise e

    def write_oneturnresult(self):
        '''
        Writes the oneturnresult file.
        '''
        # Calculate and write out the requested values
        chrom_eps = self.fort_cfg['chrom_eps']
        with open('fort.10_first_oneturn', 'r') as first:
            val_1 = first.readline().split()
        with open('fort.10_second_oneturn', 'r') as second:
            val_2 = second.readline().split()
        tunes = [chrom_eps, val_1[2], val_1[3], val_2[2], val_2[3]]
        chrom1 = (float(val_2[2]) - float(val_1[2])) / float(chrom_eps)
        chrom2 = (float(val_2[3]) - float(val_1[3])) / float(chrom_eps)
        mychrom = [chrom1, chrom2]
        # with open('fort.10_beta_oneturn', 'r') as f_in:
        #     beta = f_in.readline().split()
        # print(beta)
        beta_out = [val_1[4], val_1[47], val_1[5], val_1[48], val_1[2], val_1[3],
                    val_1[49], val_1[50], val_1[52], val_1[53], val_1[54], val_1[55],
                    val_1[56], val_1[57]]
        if self.fort_cfg['CHROM'] == '0':
            beta_out[6] = chrom1
            beta_out[7] = chrom2
        beta_out = beta_out + mychrom + tunes
        lines = ' '.join(map(str, beta_out))
        with open('oneturnresult', 'w') as f_out:
            f_out.write(lines)
            f_out.write('\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('task_id', type=int,
                        help='Current work unit ID')
    parser.add_argument('input_info', type=str,
                        help='Path to the config file.')
    args = parser.parse_args()
    job = PreprocessJob(args.task_id, args.input_info)
    try:
        job.run()
    except Exception as e:
        job.db.open()
        job_table = {}
        where = f"task_id={job.task_id}"
        job_table['status'] = 'incomplete'
        job_table['mtime'] = int(time.time() * 1E7)
        job.db.update('preprocess_wu'. job_table, where)
        raise e
