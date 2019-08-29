import os
import io
import time
import copy
import shutil
import logging
import getpass
import itertools
import configparser
# from importlib.machinery import SourceFileLoader
from collections import OrderedDict
from collections.abc import Iterable

#from . import dbtypedict
from . import utils
from . import gather
from . import constants
from . import submission
from .pysixdb import SixDB
from .dbtable import Table


class Study(object):

    def __init__(self, name='example_study', loc=os.getcwd()):
        '''Constructor'''
        self._logger = logging.getLogger(__name__)
        self.name = name
        self.location = os.path.abspath(loc)
        self.study_path = os.path.join(self.location, self.name)
        self.config = configparser.ConfigParser()
        self.config.optionxform = str  # preserve case
        self.submission = None
        self.db_info = {}
        self.type_dict = None
        # All the requested parameters for a study
        self.paths = {}
        self.env = {}
        self.madx_params = OrderedDict()
        self.madx_input = {}
        self.madx_output = {}
        self.oneturn_sixtrack_params = OrderedDict()
        self.oneturn_sixtrack_input = {}
        self.sixtrack_params = OrderedDict()
        self.sixtrack_input = {}
        self.synonym_map = {}
        self.preprocess_output = {}
        self.sixtrack_output = []
        self.tables = {}
        self.table_keys = {}
        self.pragma = OrderedDict()
        self.boinc_vars = OrderedDict()
        # initialize default values
        Study._defaults(self)
        Study._structure(self)

    def _defaults(self):
        '''initialize a study with some default settings'''
        # full path to madx
        self.paths["madx_exe"] = "/afs/cern.ch/user/m/mad/bin/madx"
        # full path to sixtrack
        self.paths["sixtrack_exe"] = "/afs/cern.ch/project/sixtrack/build/sixtrack"
        self.paths["study_path"] = self.study_path
        self.paths["preprocess_in"] = os.path.join(
            self.study_path, "preprocess_input")
        self.paths["preprocess_out"] = os.path.join(
            self.study_path, "preprocess_output")
        self.paths["sixtrack_in"] = os.path.join(
            self.study_path, "sixtrack_input")
        self.paths["sixtrack_out"] = os.path.join(
            self.study_path, "sixtrack_output")
        self.paths["templates"] = self.study_path
        # self.paths["boinc_spool"] = "/afs/cern.ch/work/b/boinc/boinc"
        self.env['test_turn'] = 1000
        self.env['surv_percent'] = 1
        self.oneturn = True
        self.collimation = False
        self.checkpoint_restart = False
        self.start_point = 1000
        self.prolong_turn = 1000
        self.cluster_class = submission.HTCondor

        self.madx_output = {
            'fc.2': 'fort.2',
            'fc.3': 'fort.3.mad',
            'fc.3.aux': 'fort.3.aux',
            'fc.8': 'fort.8',
            'fc.16': 'fort.16',
            'fc.34': 'fort.34'}
        self.oneturn_sixtrack_params = OrderedDict([
            ("turnss", 1),
            ("nss", 1),
            ("ax0s", 0.1),
            ("ax1s", 0.1),
            ("imc", 1),
            ("iclo6", 2),
            ("writebins", 1),
            ("ratios", 1),
            ("Runnam", 'FirstTurn'),
            ("idfor", 0),
            ("ibtype", 0),
            ("ition", 0),
            ("CHRO", '/'),
            ("TUNE", '/'),
            ("POST", 'POST'),
            ("POS1", ''),
            ("ndafi", 1),
            ("tunex", 62.28),
            ("tuney", 60.31),
            ("inttunex", 62.28),
            ("inttuney", 60.31),
            ("DIFF", '/DIFF'),
            ("DIF1", '/'),
            ("pmass", constants.PROTON_MASS),
            ("emit_beam", 3.75),
            ("e0", 7000),
            ("ilin", 2),
            ("EI", 3.75),
            ("bunch_charge", 1.15E11),
            ("CHROM", 0),
            ("chrom_eps", 0.000001),
            ("dp1", 0.000001),
            ("dp2", 0.000001),
            ("chromx", 2),
            ("chromy", 2)])
        self.oneturn_sixtrack_input['input'] = copy.deepcopy(self.madx_output)
        self.sixtrack_output = ['fort.10']

        self.db_info['db_type'] = 'sql'
        self.db_settings = {
            # 'synchronous': 'off',
            'foreign_keys': 'on',
            'journal_mode': 'memory',
            'auto_vacuum': 'full',
            'temp_store': 'memory',
            'count_changes': 'off'}

        self.boinc_vars['workunitName'] = 'pysixdesk'
        self.boinc_vars['fpopsEstimate'] = 30 * 2 * 10e5 / 2 * 10e6 * 6
        self.boinc_vars['fpopsBound'] = self.boinc_vars['fpopsEstimate'] * 1000
        self.boinc_vars['memBound'] = 100000000
        self.boinc_vars['diskBound'] = 200000000
        self.boinc_vars['delayBound'] = 2400000
        self.boinc_vars['redundancy'] = 2
        self.boinc_vars['copies'] = 2
        self.boinc_vars['errors'] = 5
        self.boinc_vars['numIssues'] = 5
        self.boinc_vars['resultsWithoutConcensus'] = 3
        self.boinc_vars['appName'] = 'sixtrack'
        self.boinc_vars['appVer'] = 50205

    def _structure(self):
        '''Structure the workspace of this study.
        Copy the required template files.
        '''
        temp = self.paths["templates"]
        if not os.path.isdir(temp) or not os.listdir(temp):
            if not os.path.exists(temp):
                os.makedirs(temp)
            tem_path = os.path.join(utils.PYSIXDESK_ABSPATH, 'templates')
            if os.path.isdir(tem_path) and os.listdir(tem_path):
                for item in os.listdir(tem_path):
                    s = os.path.join(tem_path, item)
                    d = os.path.join(temp, item)
                    if os.path.isfile(s):
                        shutil.copy2(s, d)
                content = "Copy templates from default source templates folder!"
                self._logger.info(content)

            else:
                content = "The default source templates folder %s is invalid!" % tem_path
                raise NotADirectoryError(content)

    def customize(self):
        '''Update the column names of database tables  and initialize the
        submission module after the user define the necessary variables.
        '''
        if not os.path.isdir(self.paths["preprocess_in"]):
            os.makedirs(self.paths["preprocess_in"])
        if not os.path.isdir(self.paths["preprocess_out"]):
            os.makedirs(self.paths["preprocess_out"])
        if not os.path.isdir(self.paths["sixtrack_in"]):
            os.makedirs(self.paths["sixtrack_in"])
        if not os.path.isdir(self.paths["sixtrack_out"]):
            os.makedirs(self.paths["sixtrack_out"])

        stp = self.study_path
        studies = os.path.dirname(stp)
        wu_path = os.path.dirname(studies)
        wu_name = os.path.basename(wu_path)
        st_name = os.path.basename(stp)

        db_type = self.db_info['db_type']
        if db_type.lower() == 'sql':
            table = Table(self.tables, self.table_keys, 'sql')
            self.db_info['db_name'] = os.path.join(self.study_path, 'data.db')
        elif db_type.lower() == 'mysql':
            table = Table(self.tables, self.table_keys, 'mysql')
            self.db_info['db_name'] = wu_name + '_' + st_name
        else:
            content = "Unknown database type %s! Must be either 'sql' or 'mysql'." % db_type
            raise ValueError(content)

        self.st_pre = wu_name + '_' + st_name
        boinc_spool = self.paths['boinc_spool']
        self.env['boinc_work'] = os.path.join(boinc_spool, self.st_pre, 'work')
        self.env['boinc_results'] = os.path.join(
            boinc_spool, self.st_pre, 'results')

        # initialize the database table
        table.init_preprocess_tables()
        table.init_sixtrack_tables()
        table.init_state_tables()
        if self.oneturn:
            self.preprocess_output['oneturnresult'] = 'oneturnresult'
            table.init_oneturn_tables()
            table.customize_tables('oneturn_sixtrack_wu',
                    self.oneturn_sixtrack_params)
            table.customize_tables('templates',
                    list(self.oneturn_sixtrack_input['temp']), 'BLOB')

        if self.collimation:
            self.preprocess_output['fort3.limi'] = 'fort3.limi'
            table.init_collimation_tables()

        table.customize_tables('templates', list(self.madx_input.keys()),
                'BLOB')
        table.customize_tables('templates', list(self.sixtrack_input['temp']),
                'BLOB')
        table.customize_tables('env', self.env)
        table.customize_tables('env', list(self.paths.keys()), 'text')
        table.customize_tables('preprocess_wu', self.madx_params)
        table.customize_tables('preprocess_task',
                list(self.preprocess_output.values()), 'MEDIUMBLOB')
        table.customize_tables('sixtrack_wu', self.sixtrack_params)
        table.customize_tables('sixtrack_task', list(self.sixtrack_output), 'MEDIUMBLOB')
        table.customize_tables('boinc_vars', self.boinc_vars)

        # Initialize the database
        self.db = SixDB(self.db_info, settings=self.db_settings, create=True)
        # create the database tables if not exist
        if not self.db.fetch_tables():
            self.db.create_tables(self.tables, self.table_keys)

        # Initialize the submission object
        try:
            self.submission = self.cluster_class(self.paths['templates'])
        except Exception:
            content = 'Failed to instantiate cluster class.'
            self._logger.error(content, exc_info=True)
            raise

    def update_db(self):
        '''Update the database whith the user-defined parameters'''
        temp = self.paths["templates"]
        cont = os.listdir(temp)
        require = []
        #if self.oneturn:
        #    require += self.oneturn_sixtrack_input["temp"]
        require += self.sixtrack_input['temp']
        require.append(self.madx_input["mask_file"])
        for r in require:
            if r not in cont:
                content = "The required file %s isn't found in %s!" % (r, temp)
                raise FileNotFoundError(content)
        outputs = self.db.select('templates', self.tables['templates'].keys())
        if not outputs:
            tab = {}
            for key, value in self.madx_input.items():
                value = os.path.join(self.study_path, value)
                tab[key] = utils.compress_buf(value)
            #if self.oneturn:
            #    for key in self.oneturn_sixtrack_input['temp']:
            #        value = os.path.join(self.study_path, key)
            #        tab[key] = utils.compress_buf(value)
            for key in self.sixtrack_input['temp']:
                value = os.path.join(self.study_path, key)
                tab[key] = utils.compress_buf(value)
            self.db.insert('templates', tab)
        outputs = self.db.select('env', self.paths.keys())
        envs = {}
        envs.update(self.paths)
        envs.update(self.env)
        if not outputs:
            self.db.insert('env', envs)
        else:
            self.db.update('env', envs)
        outputs = self.db.select('boinc_vars', self.boinc_vars.keys())
        if not outputs:
            self.db.insert('boinc_vars', self.boinc_vars)
        else:
            self.db.update('boinc_vars', self.boinc_vars)

        self.config.clear()
        self.config['madx'] = {}
        madx_sec = self.config['madx']
        self.config['mask'] = {}
        mask_sec = self.config['mask']
        madx_sec['source_path'] = self.paths['templates']
        madx_sec['madx_exe'] = self.paths['madx_exe']
        madx_sec['mask_file'] = self.madx_input["mask_file"]
        madx_sec['oneturn'] = str(self.oneturn)
        madx_sec['collimation'] = str(self.collimation)
        inp = self.madx_output
        madx_sec['output_files'] = utils.encode_strings(inp)
        if self.oneturn:
            self.config['sixtrack'] = {}
            cus_sec = self.config['sixtrack']
            self.config['oneturn_sixtrack_results'] = self.tables[
                    'oneturn_sixtrack_results']
            self.config['fort3'] = self.oneturn_sixtrack_params
            cus_sec['source_path'] = self.paths['templates']
            cus_sec['sixtrack_exe'] = self.paths['sixtrack_exe']
            inp = self.oneturn_sixtrack_input['temp']
            cus_sec['temp_files'] = utils.encode_strings(inp)
            inp = self.oneturn_sixtrack_input['input']
            cus_sec['input_files'] = utils.encode_strings(inp)
        if self.collimation:
            self.config['collimation'] = {}
            cus_sec = self.config['collimation']
            cus_sec['source_path'] = self.paths['templates']
            inp = self.collimation_input
            cus_sec['input_files'] = utils.encode_strings(inp)

        keys = list(self.madx_params.keys())
        values = []
        for key in keys:
            val = self.madx_params[key]
            if not isinstance(val, Iterable) or isinstance(val, str):
                val = [val]  # wrap with list for a single element
            values.append(val)

        check_params = self.db.select('preprocess_wu', keys)
        check_jobs = self.db.select(
            'preprocess_wu', ['wu_id', 'job_name', 'status'])

        wu_id = len(check_params)
        for element in itertools.product(*values):
            madx_table = OrderedDict()
            if element in check_params:
                i = check_params.index(element)
                name = check_jobs[i][1]
                content = "The job %s is already in the database!" % name
                self._logger.warning(content)
                continue
            for i in range(len(element)):
                ky = keys[i]
                vl = element[i]
                mask_sec[ky] = str(vl)
                madx_table[ky] = vl
            prefix = self.madx_input['mask_file'].split('.')[0]
            job_name = self.name_conven(prefix, keys, element, '')
            wu_id += 1
            madx_table['wu_id'] = wu_id
            madx_sec['dest_path'] = self.paths['preprocess_out']
            cus_sec['dest_path'] = self.paths['preprocess_out']
            f_out = io.StringIO()
            self.config.write(f_out)
            out = f_out.getvalue()
            madx_table['input_file'] = utils.compress_buf(out, 'str')
            madx_table['status'] = 'incomplete'
            madx_table['job_name'] = job_name
            madx_table['mtime'] = int(time.time() * 1E7)
            self.db.insert('preprocess_wu', madx_table)
            content = 'Store preprocess job %s into database!' % job_name
            self._logger.info(content)

        # prepare sixtrack parameters in database
        self.config.clear()
        self.config['sixtrack'] = {}
        six_sec = self.config['sixtrack']
        self.config['fort3'] = {}
        fort3_sec = self.config['fort3']
        six_sec['source_path'] = self.paths['templates']
        six_sec['sixtrack_exe'] = self.paths['sixtrack_exe']
        if 'additional_input' in self.sixtrack_input.keys():
            inp = self.sixtrack_input['additional_input']
            six_sec['additional_input'] = utils.encode_strings(inp)
        inp = self.sixtrack_input['input']
        six_sec['input_files'] = utils.encode_strings(inp)
        six_sec['boinc_dir'] = self.paths['boinc_spool']
        inp = self.sixtrack_input['temp']
        six_sec['temp_files'] = utils.encode_strings(inp)
        inp = self.sixtrack_output
        six_sec['output_files'] = utils.encode_strings(inp)
        six_sec['test_turn'] = str(self.env['test_turn'])
        tracking_turn = self.sixtrack_params[self.synonym_map['tracking_turn']]
        six_sec['tracking_turn'] = str(tracking_turn)
        self.config['six_results'] = self.tables['six_results']
        if self.collimation:
            self.config['aperture_losses'] = self.tables['aperture_losses']
            self.config['collimation_losses'] = self.tables['collimation_losses']
            self.config['init_state'] = self.tables['init_state']
            self.config['final_state'] = self.tables['final_state']

        madx_keys = list(self.madx_params.keys())
        madx_vals = self.db.select('preprocess_wu', ['wu_id'] + madx_keys)
        if not madx_vals:
            content = "The preprocess_wu table is empty!"
            self._logger.warning(content)
            return
        madx_vals = list(zip(*madx_vals))
        madx_ids = list(madx_vals[0])
        madx_params = madx_vals[1:]
        keys = list(self.sixtrack_params.keys())
        values = []
        for key in keys:
            val = self.sixtrack_params[key]
            if not isinstance(val, Iterable) or isinstance(val, str):
                val = [val]  # wrap with list for a single element
            values.append(val)

        keys.append('preprocess_id')
        values.append(madx_ids)
        outputs = self.db.select('sixtrack_wu', keys)
        namevsid = self.db.select('sixtrack_wu', ['wu_id', 'job_name'],
                DISTINCT=True)
        wu_id = len(namevsid)
        for element in itertools.product(*values):
            job_table = OrderedDict()
            a = []
            for i in element:
                if isinstance(i, Iterable):
                    i = str(i)
                a.append(i)
            element = tuple(a)
            if element in outputs:
                i = outputs.index(element)
                nm = namevsid[i][1]
                content = "The sixtrack job %s is already in the database!" % nm
                self._logger.info(content)
                continue
            for i in range(len(element) - 1):
                ky = keys[i]
                vl = element[i]
                fort3_sec[ky] = str(vl)
                if isinstance(vl, Iterable):
                    vl = str(vl)
                job_table[ky] = vl
            vl = element[len(element) - 1]  # the last one is madx_id(wu_id)
            j = madx_ids.index(vl)
            for k in range(len(madx_params)):
                ky = madx_keys[k]
                vl = madx_params[k][j]
                fort3_sec[ky] = str(vl)
            job_table['preprocess_id'] = j + 1  # in db id begin from 1
            wu_id += 1
            job_table['wu_id'] = wu_id
            job_table['tracking_turn'] = tracking_turn
            job_name = 'sixtrack_job_preprocess_id_%i_wu_id_%i' % (j + 1,
                    wu_id)
            job_table['job_name'] = job_name
            six_sec['dest_path'] = self.paths['sixtrack_out']
            self.config['boinc'] = self.boinc_vars
            f_out = io.StringIO()
            self.config.write(f_out)
            out = f_out.getvalue()
            job_table['input_file'] = utils.compress_buf(out, 'str')
            job_table['status'] = 'incomplete'
            job_table['mtime'] = int(time.time() * 1E7)
            self.db.insert('sixtrack_wu', job_table)
            content = 'Store sixtrack job %s into database!' % job_name
            self._logger.info(content)

    def info(self, job=2, where=None):
        '''Print the status information of this study.
        job=
        0: print madx, oneturn sixtrack job
        1: print sixtrack job
        2: print madx, oneturn sixtrack and sixtrack jobs
        where: the filter condition for database query, e.g. "status='complete'"'''
        query = ['wu_id', 'job_name', 'status', 'unique_id']
        if job == 0 or job == 2:
            wus = self.db.select('preprocess_wu', query, where)
            print('madx and one turn sixtrack jobs:')
            print(query)
            for i in wus:
                print(i)
        if job == 1 or job == 2:
            six = self.db.select('sixtrack_wu', query, where)
            print('Sixtrack jobs:')
            print(query)
            for j in six:
                print(j)

    def submit(self, typ, trials=5, *args, **kwargs):
        '''Sumbit the preporcess or sixtrack jobs to htctondor.
        @type(0,1 or 2) The job type, 0 is preprocess job, 1 is sixtrack job,
        2 is collimation job
        @trials The maximum number of trials of submission
        '''
        if typ == 0:
            input_path = self.paths['preprocess_in']
            jobname = 'preprocess'
            table_name = 'preprocess_wu'
        elif typ == 1:
            input_path = self.paths['sixtrack_in']
            jobname = 'sixtrack'
            table_name = 'sixtrack_wu'
        else:
            content = ("Unknown job type %s, must be either 0 "
                       "(preprocess job) or 1 (tracking job)") % typ
            raise ValueError(content)

        batch_name = os.path.join(self.study_path, jobname)
        where = "batch_name like '%s_%%'" % batch_name
        que_out = self.db.select(table_name, 'batch_name',
                                 where, DISTINCT=True)
        ibatch = len(que_out)
        ibatch += 1
        batch_name = batch_name + '_' + str(ibatch)

        status, out = self.submission.submit(input_path, batch_name, trials,
                *args, **kwargs)

        if status:
            content = "Submit %s job successfully!" % jobname
            self._logger.info(content)
            table = {}
            table['status'] = 'submitted'
            for ky, vl in out.items():
                where = 'task_id=%s' % ky
                table['unique_id'] = vl
                table['batch_name'] = batch_name
                self.db.update(table_name, table, where)
        else:
            content = "Failed to submit %s job!" % jobname
            self._logger.error(content)

    def collect_result(self, typ, boinc=False):
        '''Collect the results of preprocess or  sixtrack jobs'''
        config ={}
        info_sec = {}
        config['info'] = info_sec
        config['db_setting'] = self.db_settings
        config['db_info'] = self.db_info

        if typ == 0:
            if self.oneturn:
                # The section name should be same with the table name
                config['oneturn_sixtrack_results'] = self.tables[
                        'oneturn_sixtrack_results']
            info_sec['path'] = self.paths['preprocess_out']
            fileout = list(self.preprocess_output.values())
            info_sec['outs'] = Table.result_table(fileout)
        elif typ == 1:
            config['six_results'] = self.tables['six_results']
            info_sec['path'] = self.paths['sixtrack_out']
            info_sec['boinc_results'] = self.env['boinc_results']
            info_sec['boinc'] = boinc
            info_sec['st_pre'] = self.st_pre
            info_sec['outs'] = Table.result_table(self.sixtrack_output)
            if self.collimation:
                config['aperture_losses'] = self.tables['aperture_losses']
                config['collimation_losses'] = self.tables['collimation_losses']
                config['init_state'] = self.tables['init_state']
                config['final_state'] = self.tables['final_state']
        else:
            content = "Unkown job type %s" % typ
            raise ValueError(content)

        try:
            gather.run(typ, config, self.submission)
        except Exception as e:
            raise e

    def prepare_sixtrack_input(self, boinc=False, *args, **kwargs):
        '''Prepare the input files for sixtrack job'''
        if self.checkpoint_restart:
            self.prepare_cr()
        where = "status='complete'"
        preprocess_outs = self.db.select(
            'preprocess_wu', ['wu_id', 'task_id'], where)
        if not preprocess_outs:
            content = "There isn't complete madx job!"
            self._logger.warning(content)
            return
        preprocess_outs = list(zip(*preprocess_outs))
        if len(preprocess_outs[0]) == 1:
            where = "status='incomplete' and preprocess_id=%s" % str(preprocess_outs[0][0])
        else:
            where = "status='incomplete' and preprocess_id in %s" % str(
                preprocess_outs[0])
        outputs = self.db.select('sixtrack_wu', ['wu_id', 'preprocess_id',
            'input_file', 'job_name', 'tracking_turn'], where)
        if not outputs:
            content = "There isn't available sixtrack job to submit!"
            self._logger.info(content)
            return
        outputs = list(zip(*outputs))
        wu_ids = []
        tracking_turns = []
        pre_ids = []
        job_names = []
        input_buf_new = []
        for wu_id, pre_id, buf, job_name, n in zip(outputs[0], outputs[1],
                                                outputs[2], outputs[3],
                                                outputs[4]):
            in_fil = utils.decompress_buf(buf, None, 'buf')
            self.config.clear()
            self.config.read_string(in_fil)
            paramsdict = self.config['fort3']
            status = self.pre_calc(paramsdict, pre_id)  # further calculation
            if status:
                f_out = io.StringIO()
                self.config.write(f_out)
                out = f_out.getvalue()
                buf_new = utils.compress_buf(out, 'str')
                input_buf_new.append(buf_new)
                wu_ids.append(wu_id)
                tracking_turns.append(n)
                pre_ids.append(pre_id)
                job_names.append(job_name)
        if not wu_ids:
            content = ("There isn't available sixtrack job to submit due to "
                       "failed further calculation!")
            # self._logger.error(content)
            raise Exception(content)
        task_table = {}
        wu_table = {}
        task_ids = []
        for wu_id in wu_ids:
            task_table['wu_id'] = wu_id
            task_table['mtime'] = int(time.time() * 1E7)
            self.db.insert('sixtrack_task', task_table)
            where = "mtime=%s and wu_id=%s" % (task_table['mtime'], wu_id)
            task_id = self.db.select('sixtrack_task', ['task_id'], where)
            task_id = task_id[0][0]
            task_ids.append(task_id)
            wu_table['task_id'] = task_id
            wu_table['mtime'] = int(time.time() * 1E7)
            where = "wu_id=%s" % wu_id
            self.db.update('sixtrack_wu', wu_table, where)
        db_info = {}
        db_info.update(self.db_info)
        tran_input = []
        if db_info['db_type'].lower() == 'sql':
            sub_name = os.path.join(self.paths['sixtrack_in'], 'sub.db')
            sub_main = self.db_info['db_name']
            if os.path.exists(sub_name):
                os.remove(sub_name)  # remove the old one
            shutil.copy2(sub_main, sub_name)
            db_info['db_name'] = sub_name
            sub_db = SixDB(db_info, self.db_settings)

            sub_db.drop_table('sixtrack_task')
            sub_db.drop_table('result')
            sub_db.drop_table('sixtrack_wu')
            sub_db.drop_table('oneturn_sixtrack_wu')
            sub_db.drop_table('templates')
            tables = {'wu_id': 'int', 'tracking_turn': 'int',
                    'preprocess_id': 'int', 'task_id': 'int',
                    'input_file': 'blob', 'boinc': 'text', 'job_name': 'text'}
            sub_db.create_table('sixtrack_wu', tables)
            incom_job = {}
            incom_job['wu_id'] = wu_ids
            incom_job['tracking_turn'] = tracking_turns
            incom_job['preprocess_id'] = pre_ids
            incom_job['task_id'] = task_ids
            incom_job['input_file'] = input_buf_new
            incom_job['job_name'] = job_names
            incom_job['boinc'] = ['false'] * len(wu_ids)
            if boinc:
                incom_job['boinc'] = ['true'] * len(wu_ids)
            sub_db.insertm('sixtrack_wu', incom_job)
            # wu_ids = sub_db.select('sixtrack_wu', ['wu_id'])
            # wu_ids = list(zip(*wu_ids))[0]
            sub_db.close()
            db_info['db_name'] = 'sub.db'
            content = "The submitted db %s is ready!" % self.db_info['db_name']
            self._logger.info(content)
            tran_input.append(sub_name)
        else:
            job_table = {}
            input_list = dict(zip(task_ids, input_buf_new))
            for task_id in task_ids:
                where = "task_id=%s" % task_id
                job_table['boinc'] = str(boinc)
                job_table['input_file'] = input_list[task_id]
                self.db.update('sixtrack_wu', job_table, where)
        if boinc:
            self.init_boinc_dir()
        input_info = os.path.join(self.paths['sixtrack_in'], 'db.ini')
        self.config.clear()
        self.config['db_info'] = db_info
        with open(input_info, 'w') as f_out:
            self.config.write(f_out)
        tran_input.append(input_info)
        in_path = self.paths['sixtrack_in']
        out_path = self.paths['sixtrack_out']
        exe = os.path.join(utils.PYSIXDESK_ABSPATH, 'pysixdesk/lib', 'sixtrack.py')
        self.submission.prepare(task_ids, tran_input, exe, 'db.ini', in_path,
                                out_path, flavour='tomorrow', *args, **kwargs)

    def prepare_preprocess_input(self, *args, **kwargs):
        '''Prepare the input files for madx and one turn sixtrack job'''
        where = "status='incomplete'"
        outputs = self.db.select(
            'preprocess_wu', ['wu_id', 'input_file'], where)
        if not outputs:
            content = "There isn't incomplete preprocess job!"
            self._logger.warning(content)
            return
        trans = []
        outputs = list(zip(*outputs))
        wu_ids = outputs[0]
        task_table = {}
        wu_table = {}
        task_ids = []
        for wu_id in wu_ids:
            task_table['wu_id'] = wu_id
            task_table['mtime'] = int(time.time() * 1E7)
            self.db.insert('preprocess_task', task_table)
            where = "mtime=%s and wu_id=%s" % (task_table['mtime'], wu_id)
            task_id = self.db.select('preprocess_task', ['task_id'], where)
            task_id = task_id[0][0]
            task_ids.append(task_id)
            wu_table['task_id'] = task_id
            wu_table['mtime'] = int(time.time() * 1E7)
            where = "wu_id=%s" % wu_id
            self.db.update('preprocess_wu', wu_table, where)
        db_info = {}
        db_info.update(self.db_info)
        if db_info['db_type'].lower() == 'sql':
            sub_name = os.path.join(self.paths['preprocess_in'], 'sub.db')
            if os.path.exists(sub_name):
                os.remove(sub_name)  # remove the old one
            db_info['db_name'] = sub_name
            sub_db = SixDB(db_info, settings=self.db_settings, create=True)
            sub_db.create_table('preprocess_wu', {'wu_id': 'int',
                                                  'task_id': 'int',
                                                  'input_file': 'blob'})
            incom_job = {}
            incom_job['wu_id'] = outputs[0]
            incom_job['task_id'] = task_ids
            incom_job['input_file'] = outputs[1]
            sub_db.insertm('preprocess_wu', incom_job)
            sub_db.close()
            db_info['db_name'] = 'sub.db'
            content = "The submitted database %s is ready!" % db_info['db_name']
            self._logger.info(content)
            trans.append(sub_name)

        input_info = os.path.join(self.paths['preprocess_in'], 'db.ini')
        self.config.clear()
        self.config['db_info'] = db_info
        with open(input_info, 'w') as f_out:
            self.config.write(f_out)
        trans.append(input_info)
        in_path = self.paths['preprocess_in']
        out_path = self.paths['preprocess_out']
        exe = os.path.join(utils.PYSIXDESK_ABSPATH, 'pysixdesk/lib', 'preprocess.py')
        self.submission.prepare(task_ids, trans, exe, 'db.ini', in_path,
                                out_path, flavour='espresso', *args, **kwargs)

    def pre_calc(self, **kwargs):
        '''Further calculations for the specified parameters'''
        pass

    def prepare_cr(self):
        '''Prepare the checkpoint data, add new lines in db'''
        pass
        where = f"status='complete' and tracking_turn={self.start_point}"
        results = self.db.select('sixtrack_wu', where)
        if not results:
            self._logger.warning(f"There isn't complete job with tracking\
                    turn {self.start_point}")
            return False
        #raise Exception

    def init_boinc_dir(self):
        '''Initialise the boinc directory'''
        user_name = getpass.getuser()
        work_path = self.env['boinc_work']
        results_path = self.env['boinc_results']
        boinc_dir = os.path.dirname(work_path)
        owner = os.path.join(boinc_dir, 'owner')
        if not os.path.isdir(boinc_dir):
            os.mkdir(boinc_dir)
            os.system('fs setacl -dir %s -acl %s rlidwka boinc:users rl'
                      % (boinc_dir, user_name))
        if not os.path.isfile(owner):
            with open(owner, 'w') as f_out:
                f_out.write(user_name)
        if not os.path.isdir(work_path):
            os.mkdir(work_path)
        if not os.path.isdir(results_path):
            os.mkdir(results_path)

    def purge_table(self, table_name):
        '''Clean the invalid lines in the specified table'''
        where = "status IS NULL"
        self.db.remove(table_name, where)

    def name_conven(self, prefix, keys, values, suffix=''):
        '''The convention for naming input file'''
        b = ''
        if len(keys) == len(values):
            a = ['_'.join(map(str, i)) for i in zip(keys, values)]
            b = '_'.join(map(str, a))
        else:
            content = "The input list keys and values must have same length!"
            self._logger.error(content)
        mk = prefix + '_' + b + suffix
        return mk
