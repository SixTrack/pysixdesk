import os
import time
import shutil

from pathlib import Path
from abc import abstractmethod
from contextlib import contextmanager

from . import utils


class SixtrackJob:
    '''
    Helper class which contains some function which are common to both the
    preprocessing job and the tracking job. There are still a few similar
    methods in both jobs which could be refactored here if needed.
    '''
    @abstractmethod
    def __init__(self, wu_id, db_name):
        pass

    @abstractmethod
    def run(self):
        pass

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
            FileNotFoundError: If missing input file during symlinking
            ValueError: If provided sixtrack 'input_files' are incorect
        """
        # get the fort file patterns and values
        if source_prefix is not None:
            source_prefix = Path(source_prefix)

        fort_file = Path(self.six_cfg["fort_file"])
        patterns = ['%' + a for a in fort_cfg.keys()]
        values = list(fort_cfg.values())
        dest = fort_file.with_suffix('.temp')

        source = fort_file
        if source_prefix is not None:
            source = source_prefix / source

        try:
            utils.replace(patterns, values, source, dest)
        except Exception:
            content = "Failed to generate input file for sixtrack!"
            raise Exception(content)

        # prepare the other input files
        try:
            input_files = utils.decode_strings(self.six_cfg["input_files"])
        except Exception:
            content = "Wrong setting of oneturn sixtrack input!"
            raise ValueError(content)

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

    def sixtrack_run(self, job_name):
        """Runs sixtrack.

        Args:
            job_name (str): name of the sixtrack job.
        """
        # actually run
        six_output = os.popen(self.six_cfg["sixtrack_exe"])
        self._logger.info('Sixtrack is running...')
        # write stdout to file
        outputlines = six_output.readlines()
        self._logger.info('Sixtrack is done!')
        output_name = Path.cwd().parent / (job_name + '.output')
        with open(output_name, 'w') as six_out:
            six_out.writelines(outputlines)
        print(''.join(outputlines))

    @abstractmethod
    def _push_to_db_results(self, dest_path):
        pass

    def push_to_db(self, dest_path, job_type='preprocess'):
        """Pushes job results to database.

        Args:
            dest_path (str): Location of the job results.
            job_type (str, optional): type of job currently being run, is used
            to determine which table in the db to write the results i.e.
            '{job_type}_wu'.

        Raises:
            Exception: if failure to push results to database. Will set status
            of current wu_id to incomplete.
        """
        # reconnect database
        self.db.open()
        try:
            job_table = {}

            task_table = self._push_to_db_results(dest_path)

            if task_table['status'] == 'Success':
                job_table['status'] = 'complete'
                job_table['mtime'] = int(time.time() * 1E7)
                self.db.update(f'{job_type}_wu', job_table, f'wu_id={self.wu_id}')
                content = f"{job_type} job {self.wu_id} has completed normally!"
                self._logger.info(content)
            else:
                job_table['status'] = 'incomplete'
                job_table['mtime'] = int(time.time() * 1E7)
                self.db.update(f'{job_type}_wu', job_table, f'wu_id={self.wu_id}')
                content = "This is a failed job!"
                self._logger.warning(content)
        except Exception as e:
            job_table['status'] = 'incomplete'
            job_table['mtime'] = int(time.time() * 1E7)
            self.db.update(f'{job_type}_wu', job_table, f'wu_id={self.wu_id}')
            self._logger.error('Error during reconnection.')
            raise e
        finally:
            self.db.close()

    def sixtrack_copy_input(self, extra=[]):
        '''
        Copies the fort.3 file and other additional inputs to the cwd.

        Args:
            extra (list, optional): Any additional inputs to copy over.
        '''
        required = [self.six_cfg['fort_file']] + extra
        for infile in required:
            infi = Path(self.six_cfg['source_path']) / infile
            if infi.is_file():
                shutil.copy2(infi, infile)
            else:
                content = "The required file %s isn't found!" % infile
                raise FileNotFoundError(content)
