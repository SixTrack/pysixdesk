import os
import io
import time
import copy
import shutil
import logging
import getpass
import configparser
# from importlib.machinery import SourceFileLoader
from collections import OrderedDict
from collections.abc import Iterable

from . import dbtypedict
from . import utils
from . import gather
from . import submission
from .pysixdb import SixDB


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
        self.params = None
        self.madx_input = {}
        self.madx_output = {}
        self.oneturn_sixtrack_input = {}
        self.oneturn_sixtrack_output = []
        self.sixtrack_input = {}
        self.preprocess_ouput = {}
        self.sixtrack_output = []
        self.tables = {}
        self.table_keys = {}
        self.pragma = OrderedDict()
        self.boinc_vars = OrderedDict()
        # initialize default values
        self._defaults()
        self._structure()

    @property
    def cluster_class(self):
        return self._cluster_class

    @cluster_class.setter
    def cluster_class(self, value):
        '''
        if user sets his own cluster_class, cluster_module and cluster_name
        update
        '''
        self._cluster_class = value
        self._cluster_name = self._cluster_class.__name__
        # returns 'HTCondor'
        self._cluster_module = self._cluster_class.__module__
        # returns 'pysixtrack.submission'

    # the user cannot change these without going through the cluster_class
    # setter
    # it might be best to leave these as hidden attributes?
    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def cluster_module(self):
        return self._cluster_module

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
        self.paths["gather"] = os.path.join(self.study_path, "gather")
        self.paths["templates"] = self.study_path
        # self.paths["boinc_spool"] = "/afs/cern.ch/work/b/boinc/boinc"
        self.env['test_turn'] = 1000
        self.oneturn = True
        self.collimation = False
        self.cluster_class = submission.HTCondor

        self.madx_output = {
            'fc.2': 'fort.2',
            'fc.3': 'fort.3.mad',
            'fc.3.aux': 'fort.3.aux',
            'fc.8': 'fort.8',
            'fc.16': 'fort.16',
            'fc.34': 'fort.34'}

        self.oneturn_sixtrack_input['input'] = copy.deepcopy(self.madx_output)
        self.sixtrack_output = ['fort.10']

        self.db_info['db_type'] = 'sql'
        # Default definition of the database tables
        self.tables['templates'] = OrderedDict()
        self.tables['env'] = OrderedDict()
        self.tables['preprocess_wu'] = OrderedDict([
            ('wu_id', 'INTEGER'),
            ('job_name', 'text'),
            ('input_file', 'text'),
            ('batch_name', 'text'),
            ('unique_id', 'text'),
            ('status', 'text'),
            ('task_id', 'int'),
            ('mtime', 'bigint')])
        self.table_keys['preprocess_wu'] = {
            'primary': ['wu_id'],
            'autoincrement': ['wu_id'],
            'foreign': {},
        }
        self.tables['preprocess_task'] = OrderedDict([
            ('task_id', 'INTEGER'),
            ('wu_id', 'int'),
            ('madx_in', 'blob'),
            ('madx_stdout', 'blob'),
            ('job_stdout', 'blob'),
            ('job_stderr', 'blob'),
            ('job_stdlog', 'blob'),
            ('status', 'text'),
            ('mtime', 'bigint')])
        self.table_keys['preprocess_task'] = {
            'primary': ['task_id'],
            'autoincrement': ['task_id'],
            'foreign': {'preprocess_wu': [['wu_id'], ['wu_id']]},
        }
        self.tables['oneturn_sixtrack_wu'] = OrderedDict()
        self.tables['oneturn_sixtrack_result'] = OrderedDict([
            ('task_id', 'int'),
            ('wu_id', 'int'),
            ('betax', 'float'),
            ('betax2', 'float'),
            ('betay', 'float'),
            ('betay2', 'float'),
            ('tunex', 'float'),
            ('tuney', 'float'),
            ('chromx', 'float'),
            ('chromy', 'float'),
            ('x', 'float'),
            ('xp', 'float'),
            ('y', 'float'),
            ('yp', 'float'),
            ('z', 'float'),
            ('zp', 'float'),
            ('chromx_s', 'float'),
            ('chromy_s', 'float'),
            ('chrom_eps', 'float'),
            ('tunex1', 'float'),
            ('tuney1', 'float'),
            ('tunex2', 'float'),
            ('tuney2', 'float'),
            ('mtime', 'bigint')])
        self.tables['sixtrack_wu'] = OrderedDict([
            ('wu_id', 'INTEGER'),
            ('preprocess_id', 'int'),
            ('job_name', 'text'),
            ('input_file', 'text'),
            ('batch_name', 'text'),
            ('unique_id', 'text'),
            ('status', 'text'),
            ('task_id', 'int'),
            ('boinc', 'text'),
            ('mtime', 'bigint')])
        self.table_keys['sixtrack_wu'] = {
            'primary': ['wu_id'],
            'autoincrement': ['wu_id'],
            'foreign': {'preprocess_wu': [['preprocess_id'], ['wu_id']]},
        }
        self.tables['sixtrack_task'] = OrderedDict([
            ('task_id', 'INTEGER'),
            ('wu_id', 'int'),
            ('fort3', 'blob'),
            ('job_stdout', 'blob'),
            ('job_stderr', 'blob'),
            ('job_stdlog', 'blob'),
            ('status', 'text'),
            ('mtime', 'bigint')])
        self.table_keys['sixtrack_task'] = {
            'primary': ['task_id'],
            'autoincrement': ['task_id'],
            'foreign': {'sixtrack_wu': [['wu_id'], ['wu_id']]},
        }
        self.tables['six_results'] = OrderedDict([
            ('six_input_id', 'int'),
            ('row_num', 'int'),
            ('turn_max', 'int'),
            ('sflag', 'int'),
            ('qx', 'float'),
            ('qy', 'float'),
            ('betx', 'float'),
            ('bety', 'float'),
            ('sigx1', 'float'),
            ('sigy1', 'float'),
            ('deltap', 'float'),
            ('dist', 'float'),
            ('distp', 'float'),
            ('qx_det', 'float'),
            ('qx_spread', 'float'),
            ('qy_det', 'float'),
            ('qy_spread', 'float'),
            ('resxfact', 'float'),
            ('resyfact', 'float'),
            ('resorder', 'int'),
            ('smearx', 'float'),
            ('smeary', 'float'),
            ('smeart', 'float'),
            ('sturns1', 'int'),
            ('sturns2', 'int'),
            ('sseed', 'float'),
            ('qs', 'float'),
            ('sigx2', 'float'),
            ('sigy2', 'float'),
            ('sigxmin', 'float'),
            ('sigxavg', 'float'),
            ('sigxmax', 'float'),
            ('sigymin', 'float'),
            ('sigyavg', 'float'),
            ('sigymax', 'float'),
            ('sigxminld', 'float'),
            ('sigxavgld', 'float'),
            ('sigxmaxld', 'float'),
            ('sigyminld', 'float'),
            ('sigyavgld', 'float'),
            ('sigymaxld', 'float'),
            ('sigxminnld', 'float'),
            ('sigxavgnld', 'float'),
            ('sigxmaxnld', 'float'),
            ('sigyminnld', 'float'),
            ('sigyavgnld', 'float'),
            ('sigymaxnld', 'float'),
            ('emitx', 'float'),
            ('emity', 'float'),
            ('betx2', 'float'),
            ('bety2', 'float'),
            ('qpx', 'float'),
            ('qpy', 'float'),
            ('version', 'float'),
            ('cx', 'float'),
            ('cy', 'float'),
            ('csigma', 'float'),
            ('xp', 'float'),
            ('yp', 'float'),
            ('delta', 'float'),
            ('dnms', 'float'),
            ('trttime', 'float'),
            ('mtime', 'bigint')])
        self.tables['collimation_results'] = OrderedDict([
            ('task_id', 'int'),
            ('mtime', 'bigint')])
        self.table_keys['collimation_results'] = {
            'primary': ['task_id'],
            'foreign': {'sixtrack_task': [['task_id'], ['task_id']]},
        }
        self.table_keys['six_results'] = {
            'primary': ['six_input_id', 'row_num'],
            'foreign': {'sixtrack_task': [['six_input_id'], ['task_id']]},
        }
        self.db_settings = {
            # 'synchronous': 'off',
            'foreign_keys': 'on',
            'journal_mode': 'memory',
            'auto_vacuum': 'full',
            'temp_store': 'memory',
            'count_changes': 'off'}

        self.tables['boinc_vars'] = OrderedDict()
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
        if not os.path.isdir(self.paths["gather"]):
            os.makedirs(self.paths["gather"])

        # drop Nones from the param dictionnaries to not have a any NULL types
        # which cause problems for the database, I would welcome a more robust
        # solution.
        self.params.drop_none()

        stp = self.study_path
        studies = os.path.dirname(stp)
        wu_path = os.path.dirname(studies)
        wu_name = os.path.basename(wu_path)
        st_name = os.path.basename(stp)

        db_type = self.db_info['db_type']
        if db_type.lower() == 'sql':
            self.type_dict = dbtypedict.SQLiteDict()
            self.db_info['db_name'] = os.path.join(self.study_path, 'data.db')
        elif db_type.lower() == 'mysql':
            self.type_dict = dbtypedict.MySQLDict()
            self.db_info['db_name'] = wu_name + '_' + st_name
        else:
            content = "Unknown database type %s! Must be either 'sql' or 'mysql'." % db_type
            raise ValueError(content)

        self.st_pre = wu_name + '_' + st_name
        boinc_spool = self.paths['boinc_spool']
        self.env['boinc_work'] = os.path.join(boinc_spool, self.st_pre, 'work')
        self.env['boinc_results'] = os.path.join(
            boinc_spool, self.st_pre, 'results')
        self.env['surv_percent'] = 1

        for key, val in self.params.madx.items():
            self.tables['preprocess_wu'][key] = self.type_dict[val]
        if self.collimation:
            self.preprocess_output['fort3.limi'] = 'fort3.limi'
        for key in self.preprocess_output.values():
            self.tables['preprocess_task'][key] = 'MEDIUMBLOB'

        for key, val in self.params.oneturn.items():
            self.tables['oneturn_sixtrack_wu'][key] = self.type_dict[val]

        for key, val in list(self.params.sixtrack.items()) +\
                list(self.params.phasespace.items()):
            self.tables['sixtrack_wu'][key] = self.type_dict[val]
        for key in self.sixtrack_output:
            self.tables['sixtrack_task'][key] = 'MEDIUMBLOB'

        for key in self.madx_input.keys():
            self.tables['templates'][key] = 'BLOB'
        if self.oneturn:
            for key in self.oneturn_sixtrack_input['temp']:
                self.tables['templates'][key] = 'BLOB'
        for key in self.sixtrack_input['temp']:
            self.tables['templates'][key] = 'BLOB'

        for key in self.paths.keys():
            self.tables['env'][key] = 'TEXT'
        for key, val in self.env.items():
            self.tables['env'][key] = self.type_dict[val]

        for key, val in self.boinc_vars.items():
            self.tables['boinc_vars'][key] = self.type_dict[val]

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

    def _update_db_preprocessing(self):
        '''
        Prepares the preprocess_wu table.
        '''

        self.config.clear()
        self.config['mask'] = {}
        self.config['madx'] = {}
        self.config['madx']['source_path'] = self.paths['templates']
        self.config['madx']['madx_exe'] = self.paths['madx_exe']
        self.config['madx']['mask_file'] = self.madx_input["mask_file"]
        self.config['madx']['oneturn'] = str(self.oneturn)
        self.config['madx']['collimation'] = str(self.collimation)
        self.config['madx']['output_files'] = str(self.madx_output)
        if self.oneturn:
            self.config['oneturn'] = self.tables['oneturn_sixtrack_result']
            self.config['fort3'] = self.params.oneturn
            self.config['sixtrack'] = {}
            self.config['sixtrack']['source_path'] = self.paths['templates']
            self.config['sixtrack']['sixtrack_exe'] = self.paths['sixtrack_exe']
            self.config['sixtrack']['temp_files'] = str(self.oneturn_sixtrack_input['temp'])
            self.config['sixtrack']['input_files'] = str(self.oneturn_sixtrack_input['input'])
        if self.collimation:
            self.config['collimation'] = {}
            self.config['collimation']['source_path'] = self.paths['templates']
            self.config['collimation']['input_files'] = str(self.collimation_input)

        check_params = self.db.select('preprocess_wu',
                                      list(self.params.madx.keys()))
        check_jobs = self.db.select('preprocess_wu', ['wu_id', 'job_name'])
        wu_id = len(check_params)
        for element in utils.product_dict(**self.params.madx):
            wu_id += 1
            for k, v in element.items():
                if isinstance(v, Iterable):
                    element[k] = str(v)

            if tuple(element.values()) in check_params:
                i = check_params.index(tuple(element.values()))

                name = check_jobs[i][1]
                content = "The job %s is already in the database!" % name
                self._logger.warning(content)
                continue

            for k, v in element.items():
                self.config['mask'][k] = str(v)

            prefix = self.madx_input['mask_file'].split('.')[0]
            job_name = self.name_conven(prefix,
                                        element.keys(),
                                        element.values(),
                                        suffix='')

            element['wu_id'] = wu_id
            self.config['madx']['dest_path'] = os.path.join(
                                    self.paths['preprocess_out'], str(wu_id))
            # this is very weird, can be self.config['sixtrack'] or
            # self.config['collimation']
            self.config['sixtrack']['dest_path'] = os.path.join(
                                    self.paths['preprocess_out'], str(wu_id))
            f_out = io.StringIO()
            self.config.write(f_out)
            out = f_out.getvalue()
            element['input_file'] = out
            element['status'] = 'incomplete'
            element['job_name'] = job_name
            element['mtime'] = int(time.time() * 1E7)
            self.db.insert('preprocess_wu', element)

    def _update_db_tracking(self):
        '''
        Prepares the sixtrack_wu table and runs the parameter calculation
        queue.
        '''
        # prepare sixtrack parameters in database
        self.config.clear()
        self.config['fort3'] = {}
        self.config['sixtrack'] = {}
        self.config['f10'] = self.tables['six_results']
        self.config['sixtrack']['source_path'] = self.paths['templates']
        self.config['sixtrack']['sixtrack_exe'] = self.paths['sixtrack_exe']
        if 'additional_input' in self.sixtrack_input.keys():
            self.config['sixtrack']['additional_input'] = self.sixtrack_input['additional_input']
        self.config['sixtrack']['input_files'] = str(self.sixtrack_input['input'])
        self.config['sixtrack']['boinc_dir'] = self.paths['boinc_spool']
        self.config['sixtrack']['temp_files'] = str(self.sixtrack_input['temp'])
        self.config['sixtrack']['output_files'] = str(self.sixtrack_output)
        self.config['sixtrack']['test_turn'] = str(self.env['test_turn'])

        madx_keys = list(self.params.madx.keys())

        madx_vals = self.db.select('preprocess_wu', ['wu_id'] + madx_keys)
        if not madx_vals:
            content = "The preprocess_wu table is empty!"
            self._logger.warning(content)
            return
        madx_vals = list(zip(*madx_vals))
        madx_ids = list(madx_vals[0])
        madx_params = madx_vals[1:]

        keys = list(self.params.sixtrack.keys()) + list(self.params.phasespace.keys())
        keys.append('preprocess_id')
        check_params = self.db.select('sixtrack_wu', keys)
        check_jobs = self.db.select('sixtrack_wu', ['wu_id', 'job_name'])
        wu_id = len(check_params)
        for element in utils.product_dict(**self.params.sixtrack,
                                          **self.params.phasespace,
                                          **{'preprocess_id': madx_ids}):
            wu_id += 1
            # run calculations
            out_calc = self.params.calc(element,
                                        wu_id=element['preprocess_id'],
                                        get_val_db=self.db,
                                        require='all')
            # add to the element dict
            for k, v in out_calc.items():
                if k in element.keys():
                    element[k] = v

            for k, v in element.items():
                if isinstance(v, Iterable):
                    element[k] = str(v)

            if tuple(element.values()) in check_params:
                i = check_params.index(tuple(element.values()))
                name = check_jobs[i][1]
                content = "The sixtrack job %s is already in the database!" % name
                self._logger.info(content)
                continue

            for k, v in element.items():
                if k == 'preprocess_id':
                    j = madx_ids.index(v)
                    for ky, vl in zip(madx_keys, madx_params):
                        vl = vl[j]
                        self.config['fort3'][ky] = str(vl)
                else:
                    self.config['fort3'][k] = str(v)

            element['preprocess_id'] = j + 1  # in db id begin from 1
            element['wu_id'] = wu_id
            job_name = 'sixtrack_job_preprocess_id_%i_wu_id_%i' % (j + 1, wu_id)
            element['job_name'] = job_name
            dest_path = os.path.join(self.paths['sixtrack_out'], str(wu_id))
            self.config['sixtrack']['dest_path'] = dest_path
            self.config['boinc'] = self.boinc_vars

            f_out = io.StringIO()
            self.config.write(f_out)
            out = f_out.getvalue()
            element['input_file'] = out
            element['status'] = 'incomplete'
            element['mtime'] = int(time.time() * 1E7)
            self.db.insert('sixtrack_wu', element)
            content = 'Store sixtrack job %s into database!' % job_name
            self._logger.info(content)

    def update_db(self):
        '''Update the database whith the user-defined parameters'''
        temp = self.paths["templates"]
        cont = os.listdir(temp)
        require = []
        if self.oneturn:
            require += self.oneturn_sixtrack_input["temp"]
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
                tab[key] = utils.evlt(utils.compress_buf, [value])
            if self.oneturn:
                for key in self.oneturn_sixtrack_input['temp']:
                    value = os.path.join(self.study_path, key)
                    tab[key] = utils.evlt(utils.compress_buf, [value])
            for key in self.sixtrack_input['temp']:
                value = os.path.join(self.study_path, key)
                tab[key] = utils.evlt(utils.compress_buf, [value])
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

        self._update_db_preprocessing()

    def info(self, job=2, where=None):
        '''Print the status information of this study.
        job=
        0: print madx, oneturn sixtrack job
        1: print sixtrack job
        2: print madx, oneturn sixtrack and sixtrack jobs
        where: the filter condition for database query, e.g. "status='complete'
        "'''
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
        que_out = self.db.select(table_name, 'batch_name', where,
                                 DISTINCT=True)
        ibatch = len(que_out)
        ibatch += 1
        batch_name = batch_name + '_' + str(ibatch)

        try:
            out = self.submission.submit(input_path, batch_name,
                                         trials, *args, **kwargs)
        except Exception as e:
            content = "Failed to submit %s job!" % jobname
            self._logger.error(content)
            raise e

        content = "Submit %s job successfully!" % jobname
        self._logger.info(content)
        table = {}
        table['status'] = 'submitted'
        for ky, vl in out.items():
            where = 'wu_id=%s' % ky
            table['unique_id'] = vl
            table['batch_name'] = batch_name
            self.db.update(table_name, table, where)

    def collect_result(self, typ, boinc=False):
        '''Collect the results of preprocess or  sixtrack jobs'''
        self.config.clear()
        self.config['info'] = {}
        info_sec = self.config['info']
        self.config['db_setting'] = self.db_settings
        self.config['db_info'] = self.db_info

        info_sec['cluster_module'] = self.cluster_module
        info_sec['cluster_name'] = self.cluster_name
        if typ == 0:
            self.config['oneturn'] = self.tables['oneturn_sixtrack_result']
            info_sec['path'] = self.paths['preprocess_out']
            info_sec['outs'] = str(self.preprocess_output)
            job_name = 'collect preprocess result'
            in_name = 'preprocess.ini'
            task_input = os.path.join(self.paths['gather'], str(typ), in_name)
        elif typ == 1:
            self.config['f10'] = self.tables['six_results']
            info_sec['path'] = self.paths['sixtrack_out']
            info_sec['boinc_results'] = self.env['boinc_results']
            info_sec['boinc'] = str(boinc)
            info_sec['st_pre'] = self.st_pre
            info_sec['outs'] = str(self.sixtrack_output)
            job_name = 'collect sixtrack result'
            in_name = 'sixtrack.ini'
            task_input = os.path.join(self.paths['gather'], str(typ), in_name)
        else:
            content = "Unkown job type %s" % typ
            raise ValueError(content)

        in_path = os.path.join(self.paths['gather'], str(typ))
        if not os.path.isdir(in_path):
            os.makedirs(in_path)
        with open(task_input, 'w') as f_out:
            self.config.write(f_out)
        try:
            gather.run(typ, task_input)
        except Exception as e:
            raise e
        # TODO: Submit gather job to htcondor is error-prone, so I block it for
        #       the moment. Acctually it's dispensable.
        # elif platform is 'htcondor':
        #    tran_input =[]
        #    tran_input.append(task_input)
        #    exe = os.path.join(utils.PYSIXDESK_ABSPATH, 'lib', 'gather.py')
        #    wu_ids = [typ]
        #    self.submission.prepare(wu_ids, tran_input, exe, in_name, in_path,
        #            out_path)
        #    self.submission.submit(in_path, job_name, trials)

    def prepare_sixtrack_input(self, boinc=False, *args, **kwargs):
        '''Prepare the input files for sixtrack job'''

        self._update_db_tracking()

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
        outputs = self.db.select('sixtrack_wu',
                                 ['wu_id', 'preprocess_id',
                                  'input_file', 'job_name'],
                                 where)
        if not outputs:
            content = "There isn't available sixtrack job to submit!"
            self._logger.info(content)
            return

        outputs = list(zip(*outputs))
        wu_ids = outputs[0]
        pre_ids = outputs[1]
        input_buf = outputs[2]
        job_names = outputs[3]

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
            tables = {'wu_id': 'int', 'preprocess_id': 'int', 'task_id': 'int',
                      'input_file': 'blob', 'boinc': 'text', 'job_name': 'text'}
            sub_db.create_table('sixtrack_wu', tables)
            incom_job = {}
            incom_job['wu_id'] = wu_ids
            incom_job['preprocess_id'] = pre_ids
            incom_job['task_id'] = task_ids
            incom_job['input_file'] = input_buf
            incom_job['job_name'] = job_names
            incom_job['boinc'] = ['false'] * len(wu_ids)
            if boinc:
                incom_job['boinc'] = ['true'] * len(wu_ids)
            sub_db.insertm('sixtrack_wu', incom_job)
            wu_ids = sub_db.select('sixtrack_wu', ['wu_id'])
            wu_ids = list(zip(*wu_ids))[0]
            sub_db.close()
            db_info['db_name'] = 'sub.db'
            content = "The submitted db %s is ready!" % self.db_info['db_name']
            self._logger.info(content)
            tran_input.append(sub_name)
        else:
            job_table = {}
            job_table['boinc'] = str(boinc)
            if len(wu_ids) == 1:
                where = "wu_id=%s" % wu_ids[0]
            else:
                where = "wu_id in %s" % str(tuple(wu_ids))
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
        self.submission.prepare(wu_ids, tran_input, exe, 'db.ini', in_path,
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

        trans = []
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
        self.submission.prepare(wu_ids, trans, exe, 'db.ini', in_path,
                                out_path, flavour='espresso', *args, **kwargs)

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
