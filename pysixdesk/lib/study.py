import os
import time
import json
import shutil
import logging
import getpass
import configparser
from collections import OrderedDict
from itertools import groupby

from . import utils
from . import gather
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
        self.params = None
        self.madx_input = {}
        self.madx_output = {}
        self.oneturn_sixtrack_input = {}
        self.oneturn_sixtrack_output = []
        self.sixtrack_input = {}
        self.preprocess_output = {}
        self.sixtrack_output = []
        self.tables = {}
        self.table_keys = {}
        self.pragma = OrderedDict()
        self.boinc_vars = OrderedDict()
        # initialize default values
        self._defaults()
        self._structure()

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
        self.first_turn = 1  # first turn
        self.last_turn = 100  # last turn
        self.cluster_class = submission.HTCondor
        self.max_jobsubmit = 15000

        self.madx_output = {
            'fc.2': 'fort.2',
            'fc.3': 'fort.3.mad',
            'fc.3.aux': 'fort.3.aux',
            'fc.8': 'fort.8',
            'fc.16': 'fort.16',
            'fc.34': 'fort.34'}

        self.oneturn_sixtrack_input['input'] = dict(self.madx_output)
        self.sixtrack_input['temp'] = 'fort.3'
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
                content = ("Copying templates from default source templates "
                           "folder!")
                self._logger.info(content)

            else:
                content = (f"The default source templates folder {tem_path} "
                           "is invalid!")
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

        # drop Nones from the param dictionaries to not have a any NULL types
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
            table = Table(self.tables, self.table_keys, 'sql')
            self.db_info['db_name'] = os.path.join(self.study_path, 'data.db')
        elif db_type.lower() == 'mysql':
            table = Table(self.tables, self.table_keys, 'mysql')
            self.db_info['db_name'] = wu_name + '_' + st_name
            my_cnf = os.path.join(os.getenv('HOME'), '.my.cnf')
            if os.path.isfile(my_cnf):
                self.config.clear()
                self.config.read(my_cnf)
                client = self.config['client']
                user_name = getpass.getuser()
                self.db_info['user'] = client.get('user', fallback=user_name)
                self.db_info['passwd'] = client['password']
                self.db_info['host'] = client.get('host', fallback='127.0.0.1')
                self.db_info['port'] = client.get('port', fallback='3306')
            else:
                raise FileNotFoundError(".my.cnf in $HOME dir is needed for "
                                        "login!")
        else:
            content = "Unknown database type %s! Must be either 'sql' or 'mysql'." % db_type
            raise ValueError(content)

        self.st_pre = wu_name + '_' + st_name
        boinc_spool = self.paths['boinc_spool']
        self.env['boinc_work'] = os.path.join(boinc_spool, self.st_pre, 'work')
        self.env['boinc_results'] = os.path.join(boinc_spool, self.st_pre,
                                                 'results')

        # initialize the database table
        table.init_preprocess_tables()
        table.init_sixtrack_tables()
        table.init_state_tables()
        if self.oneturn:
            self.preprocess_output['oneturnresult'] = 'oneturnresult'
            table.init_oneturn_tables()
            table.customize_tables('oneturn_sixtrack_wu',
                                   self.params.oneturn)

            table.customize_tables('templates',
                                   ['fort_file'],
                                   'BLOB')

        if self.collimation:
            self.preprocess_output['fort3.limi'] = 'fort3.limi'
            table.init_collimation_tables()
            table.customize_tables('templates',
                                   list(self.collimation_input.keys()),
                                   'MEDIUMBLOB')

        table.customize_tables('templates', list(self.madx_input.keys()),
                               'BLOB')
        table.customize_tables('templates', ['fort_file'], 'BLOB')
        if 'additional_input' in self.sixtrack_input.keys():
            inp = self.sixtrack_input['additional_input']
            table.customize_tables('templates', inp, 'BLOB')
        table.customize_tables('env', self.env)
        table.customize_tables('env', list(self.paths.keys()), 'text')
        table.customize_tables('preprocess_wu', self.params.madx)
        table.customize_tables('preprocess_task',
                               list(self.preprocess_output.values()),
                               'MEDIUMBLOB')
        table.customize_tables('sixtrack_wu', self.params.sixtrack)
        table.customize_tables('sixtrack_wu', self.params.phasespace)

        table.customize_tables('sixtrack_task', list(self.sixtrack_output),
                               'MEDIUMBLOB')
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

    def _update_db_params(self):
        '''Populates the preprocess_wu and the sixtrack_wu tables based on the
        combination of user input parameters.
        '''
        madx_keys = self.params.madx.keys()
        six_keys = (list(self.params.sixtrack.keys()) +
                    list(self.params.phasespace.keys()))
        # preprocess_wu already in the table
        prep_entries = self.db.select('preprocess_wu', madx_keys)
        prep_wu_done = set(prep_entries)
        # these are used to keep track of the preprocess_wu_id
        # increments prep_wu_id everytime a new parameter config is seen
        # and add it to the set of seen paramter configs.
        prep_wu_seen = set()
        prep_wu_id = 0
        # sixtrack_wu already in the table
        six_entries = self.db.select('sixtrack_wu',
                                     six_keys + ['preprocess_id'])
        six_wu_done = set(six_entries)

        prep_to_be_inserted = []
        six_to_be_inserted = []
        mtime = int(time.time()) * 1E7
        # iterate over the the input combinations
        for six_wu_id, (prep_wu_entry, six_wu_entry) in enumerate(self.params.combinations(), 1):
            # this sanitization could be moved to inside the
            # self.params.combinations method
            # sanitize any tuples
            prep_wu_entry = {k: json.dumps(v) if isinstance(v, tuple) else v
                             for k, v in prep_wu_entry.items()}
            six_wu_entry = {k: json.dumps(v) if isinstance(v, tuple) else v
                            for k, v in six_wu_entry.items()}

            # TODO: is there a better way to do this ?
            # insert rows in preprocess_wu
            # keep track of current wu_id, i.e. increment every new row.
            prep_values_tuple = tuple(prep_wu_entry.values())
            if prep_values_tuple not in prep_wu_seen:
                # add a copy of the row to the prep_wu_seen
                prep_wu_seen.add(prep_values_tuple)
                prep_wu_id += 1
                # job name
                prefix = self.madx_input['mask_file'].split('.')[0]
                prep_job_name = self.name_conven(prefix,
                                                 prep_wu_entry.keys(),
                                                 prep_wu_entry.values(),
                                                 suffix='')
                # populate prepprocess_wu
                if prep_values_tuple not in prep_wu_done:
                    prep_wu_done.add(prep_values_tuple)

                    prep_wu_entry['status'] = 'incomplete'
                    prep_wu_entry['job_name'] = prep_job_name
                    prep_wu_entry['mtime'] = mtime
                    prep_wu_entry['wu_id'] = prep_wu_id
                    prep_to_be_inserted.append(prep_wu_entry)
                    # self.db.insert('preprocess_wu', prep_wu_entry)
                    self._logger.info(f"Queued {prep_job_name} for storage in "
                                      "preprocess_wu table.")
                else:
                    msg = (f"Job {prep_job_name} already in preprocess_wu "
                           "table.")
                    self._logger.warning(msg)

            six_job_name = f'sixtrack_job_preprocess_id_{prep_wu_id}_wu_id_{six_wu_id}'
            # populate sixtrack_wu
            six_values_tuple = tuple(list(six_wu_entry.values()) +
                                     [prep_wu_id])
            if six_values_tuple not in six_wu_done:
                six_wu_done.add(six_values_tuple)

                six_wu_entry['status'] = 'incomplete'
                six_wu_entry['job_name'] = six_job_name
                six_wu_entry['mtime'] = mtime
                six_wu_entry['last_turn'] = self.params.sixtrack['turnss']
                six_wu_entry['preprocess_id'] = prep_wu_id
                six_wu_entry['wu_id'] = six_wu_id
                six_to_be_inserted.append(six_wu_entry)
                self._logger.info(f"Queued {six_job_name} for storage in "
                                  "sixtrack_wu table.")
            else:
                msg = f"Job {six_job_name} already in sixtrack_wu table."
                self._logger.warning(msg)
        # insert everything at once
        # ugly way of going from list of dicts to dict of lists
        # it assumes all the dicts have the exact same keys, which should
        # always be the case.
        if prep_to_be_inserted:
            self._logger.info(f"Storing {len(prep_to_be_inserted)} preprocess "
                              "jobs in preprocess_wu.")
            self.db.insertm('preprocess_wu',
                            {k: [dic[k] for dic in prep_to_be_inserted]
                             for k in prep_to_be_inserted[0]})
        if six_to_be_inserted:
            self._logger.info(f"Storing {len(six_to_be_inserted)} tracking "
                              "jobs in sixtrack_wu.")
            self.db.insertm('sixtrack_wu',
                            {k: [dic[k] for dic in six_to_be_inserted]
                             for k in six_to_be_inserted[0]})

    def _prep_preprocessing_cfg(self):
        '''Prepares the preprocess job config dict.
        '''
        madx_keys = list(self.params.madx.keys())
        # prepare the madx config dict
        self.preprocess_config = {}
        self.preprocess_config['mask'] = {'keys': json.dumps(madx_keys)}
        self.preprocess_config['madx'] = {}
        self.preprocess_config['templates'] = {}
        self.preprocess_config['madx']['madx_exe'] = self.paths['madx_exe']
        self.preprocess_config['templates']['mask_file'] = self.madx_input["mask_file"]
        self.preprocess_config['madx']['mask_file'] = self.madx_input["mask_file"]
        self.preprocess_config['madx']['oneturn'] = json.dumps(self.oneturn)
        self.preprocess_config['madx']['collimation'] = json.dumps(self.collimation)
        self.preprocess_config['madx']['output_files'] = json.dumps(self.madx_output)
        if self.oneturn:
            self.preprocess_config['oneturn_sixtrack_results'] = self.tables['oneturn_sixtrack_results']
            # to avoid scanning oneturn jobs, just get the first value of any
            # user provided lists.
            self.preprocess_config['fort3'] = {k: v[0] if isinstance(v, list) else v
                                               for k, v in self.params.oneturn.items()}
            self.preprocess_config['sixtrack'] = {}
            self.preprocess_config['sixtrack']['sixtrack_exe'] = self.paths['sixtrack_exe']
            self.preprocess_config['templates']['fort_file'] = self.oneturn_sixtrack_input['fort_file']
            self.preprocess_config['sixtrack']['fort_file'] = self.oneturn_sixtrack_input['fort_file']
            self.preprocess_config['sixtrack']['input_files'] = json.dumps(self.oneturn_sixtrack_input['input'])
        if self.collimation:
            self.preprocess_config['collimation'] = {}
            self.preprocess_config['collimation']['input_files'] = json.dumps(self.collimation_input)
            self.preprocess_config['templates'].update(self.collimation_input)

    def _prep_sixtrack_cfg(self):
        '''Prepares the sixtrack job config dict.
        '''
        six_keys = (list(self.params.sixtrack.keys()) +
                    list(self.params.phasespace.keys()))
        # prepare sixtrack config dict
        self.sixtrack_config = {}
        self.sixtrack_config['fort3'] = {'keys': json.dumps(six_keys)}
        self.sixtrack_config['sixtrack'] = {}
        self.sixtrack_config['templates'] = {}
        self.sixtrack_config['sixtrack']['sixtrack_exe'] = self.paths['sixtrack_exe']
        if 'additional_input' in self.sixtrack_input.keys():
            self.sixtrack_config['sixtrack']['additional_input'] = json.dumps(self.sixtrack_input['additional_input'])
            self.sixtrack_config['templates'].update(zip(self.sixtrack_input['additional_input'],
                                                         self.sixtrack_input['additional_input']))
        self.sixtrack_config['sixtrack']['input_files'] = json.dumps(self.sixtrack_input['input'])
        self.sixtrack_config['sixtrack']['boinc_dir'] = self.paths['boinc_spool']
        self.sixtrack_config['sixtrack']['fort_file'] = json.dumps(self.sixtrack_input['fort_file'])
        self.sixtrack_config['templates']['fort_file'] = json.dumps(self.sixtrack_input['fort_file'])
        self.sixtrack_config['sixtrack']['output_files'] = json.dumps(self.sixtrack_output)
        self.sixtrack_config['sixtrack']['test_turn'] = json.dumps(self.env['test_turn'])
        self.sixtrack_config['six_results'] = self.tables['six_results']
        if self.collimation:
            self.sixtrack_config['aperture_losses'] = self.tables['aperture_losses']
            self.sixtrack_config['collimation_losses'] = self.tables['collimation_losses']
            self.sixtrack_config['init_state'] = self.tables['init_state']
            self.sixtrack_config['final_state'] = self.tables['final_state']
        self.sixtrack_config['boinc'] = self.boinc_vars

    def _run_calcs(self):
        '''Runs any input parameter based calculations and updates the results
        in sixtrack_wu.
        '''
        six_keys = (list(self.params.sixtrack.keys()) +
                    list(self.params.phasespace.keys()) +
                    ['wu_id', 'preprocess_id'])
        # create a dict for each row. This is bulky due to the json.loads :(
        six_wu_entries = []
        for row in self.db.select('sixtrack_wu', six_keys):
            d = {}
            for k, v in zip(six_keys, row):
                if isinstance(v, str):
                    try:
                        v = json.loads(v)
                    except json.JSONDecodeError:  # json custom exception
                        # print(f'json.loads({v}) failed.')
                        pass
                d[k] = v
            six_wu_entries.append(d)
        # prepare a map of preprocess_id to task_id
        madx_wu_task = self.db.select('preprocess_task',
                                      ['wu_id', 'task_id'])
        pre_to_task_id = dict(madx_wu_task)

        calc_out_to_be_updated = []
        where_to_be_updated = []
        # run the calculations
        for row in six_wu_entries:
            result = self.params.calc(row,
                                      task_id=pre_to_task_id[row['preprocess_id']],
                                      get_val_db=self.db,
                                      require='all')
            if result:
                # only update the columns which need updating
                update_cols = {}
                for k, v in result.items():
                    if k in row.keys():
                        update_cols[k] = v
                calc_out_to_be_updated.append(update_cols)
                where_to_be_updated.append({'wu_id': row['wu_id']})
                self._logger.info('Queued update of sixtack_wu/wu_id:'
                                  f'{row["wu_id"]} with {update_cols}.')

        if calc_out_to_be_updated:
            # update everything at once
            self._logger.info(f'Updating {len(calc_out_to_be_updated)} rows of '
                              'sixtrack_wu.')
            # turn list of dicts into dict of lists
            calc_out_to_be_updated = {k: [dic[k] for dic in calc_out_to_be_updated]
                                      for k in calc_out_to_be_updated[0]}
            where_to_be_updated = {k: [dic[k] for dic in where_to_be_updated]
                                   for k in where_to_be_updated[0]}
            self.db.updatem('sixtrack_wu',
                            calc_out_to_be_updated,
                            where=where_to_be_updated)

    def update_db(self, db_check=False):
        '''Update the database whith the user-defined parameters'''
        temp = self.paths["templates"]
        cont = os.listdir(temp)
        require = []
        require.append(self.sixtrack_input['fort_file'])
        require.append(self.madx_input["mask_file"])
        for r in require:
            if r not in cont:
                content = "The required file %s isn't found in %s!" % (r, temp)
                raise FileNotFoundError(content)
        outputs = self.db.select('templates', self.tables['templates'].keys())
        tab = {}
        for key, value in self.madx_input.items():
            value = os.path.join(self.study_path, value)
            tab[key] = utils.compress_buf(value)
        value = os.path.join(self.study_path, self.sixtrack_input['fort_file'])
        tab['fort_file'] = utils.compress_buf(value)
        if self.collimation:
            for key in self.collimation_input.keys():
                val = os.path.join(self.study_path,
                                   self.collimation_input[key])
                tab[key] = utils.compress_buf(val)
        if 'additional_input' in self.sixtrack_input.keys():
            inp = self.sixtrack_input['additional_input']
            for key in inp:
                value = os.path.join(self.study_path, key)
                tab[key] = utils.compress_buf(value)
        if not outputs:
            self.db.insert('templates', tab)
        else:
            self.db.update('templates', tab)

        outputs = self.db.select('boinc_vars', self.boinc_vars.keys())
        if not outputs:
            self.db.insert('boinc_vars', self.boinc_vars)
        else:
            self.db.update('boinc_vars', self.boinc_vars)

        # update the parameters
        outputs = self.db.select('env', self.paths.keys())
        envs = {}
        envs.update(self.paths)
        envs.update(self.env)
        if not outputs:
            self.db.insert('env', envs)
        else:
            self.db.update('env', envs)

        # update preprocess_wu and sixtrack_wu with parameter combinations.
        self._update_db_params()

    def info(self, job=2, verbose=False, where=None):
        '''Print the status information of this study.
        job=
        0: print madx, oneturn sixtrack job
        1: print sixtrack job
        2: print madx, oneturn sixtrack and sixtrack jobs
        where: the filter condition for database query, e.g.
            "status='complete'""
        '''
        query_list = ['wu_id', 'job_name', 'status', 'unique_id']
        typ = ['preprocess_wu', 'sixtrack_wu']
        titles = ['madx and one turn sixtrack jobs:', 'Sixtrack jobs:']

        def query(index):
            wus = self.db.select(typ[int(index)], query_list, where)
            content = '\n'+titles[int(index)] + '\n'
            comp = []
            subm = []
            incm = []
            if wus:
                results = dict(zip(query_list, zip(*wus)))
                for ele in results['status']:
                    if ele == 'complete':
                        comp.append(ele)
                    if ele == 'submitted':
                        subm.append(ele)
                    if ele == 'incomplete':
                        incm.append(ele)
            content += f'complete: {len(comp)} \n'
            content += f'submitted: {len(subm)} \n'
            content += f'incomplete: {len(incm)}\n'
            self._logger.info(content)
            if verbose:
                print(query_list)
                for i in wus:
                    print(i)

        if job == 0 or job == 2:
            query(0)
        if job == 1 or job == 2:
            query(1)

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

        status, out = self.submission.submit(input_path,
                                             batch_name,
                                             self.max_jobsubmit,
                                             trials,
                                             *args,
                                             **kwargs)

        if status:
            content = "Submit %s job successfully!" % jobname
            self._logger.info(content)
            table = {}
            table['status'] = 'submitted'
            self._logger.info(f"Updating the {table_name} table for job status.....")
            bar = utils.ProgressBar(len(out))
            for ky, vl in out.items():
                bar.update()
                keys = ky.split('-')
                for k in keys:
                    where = 'task_id=%s' % k
                    table['unique_id'] = vl
                    table['batch_name'] = batch_name
                    self.db.update(table_name, table, where)
        else:
            content = "Failed to submit %s job!" % jobname
            self._logger.error(content)

    def collect_result(self, typ, boinc=False):
        '''Collect the results of preprocess or sixtrack jobs'''
        config = {}
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

    def prepare_sixtrack_input(self, resubmit=False, boinc=False, groupby=None,
                               *args, **kwargs):
        '''Prepare the input files for sixtrack job'''
        # Prepares the sixtrack config dict, in the self.sixtrack_config
        # attribute
        self._prep_sixtrack_cfg()
        # Run any input paramter based calculations and update the sixtrack_wu
        # with the results.
        self._run_calcs()

        self._logger.info("Going to prepare input files for sixtrack jobs....")
        if self.checkpoint_restart:
            self.prepare_cr()

        where = "status='complete'"
        preprocess_outs = self.db.select('preprocess_wu', ['wu_id'], where)
        if not preprocess_outs:
            content = "There isn't complete madx job!"
            self._logger.warning(content)
            return
        preprocess_outs = list(zip(*preprocess_outs))

        if resubmit:
            constraints = "status='submitted'"
            action = 'resubmit'
        else:
            constraints = "status='incomplete' and preprocess_id in %s" % str(
                preprocess_outs[0]).replace(',)', ')')  # if there is a single
            # job, the comma of the tuple (1,) breaks the sql query because
            # there is no proper sql sanitization...
            action = 'submit'
        names = self.tables['sixtrack_wu'].keys()
        outputs = self.db.select('sixtrack_wu',
                                 names,
                                 constraints)
        if not outputs:
            content = f"There isn't available sixtrack job to {action}!"
            self._logger.info(content)
            return

        outputs = dict(zip(names, zip(*outputs)))
        outputs['boinc'] = ['false'] * len(outputs['wu_id'])
        if boinc:
            outputs['boinc'] = ['true'] * len(outputs['wu_id'])

        task_table = {}
        wu_table = {}
        task_ids = OrderedDict()
        mtime = int(time.time() * 1E7)
        task_table['wu_id'] = []
        task_table['last_turn'] = []
        task_table['mtime'] = []
        self._logger.info("creating new lines in sixtrack_task table.....")
        bar = utils.ProgressBar(len(outputs['last_turn']))
        for wu_id, last_turn in zip(outputs['wu_id'], outputs['last_turn']):
            bar.update()
            where = f'wu_id={wu_id} and last_turn={last_turn} and status is null'
            chck = self.db.select('sixtrack_task', ['task_id'], where)
            if chck:
                task_id = chck[0][0]
                task_ids[task_id] = (wu_id, last_turn)
            else:
                task_table['wu_id'].append(wu_id)
                task_table['last_turn'].append(last_turn)
                task_table['mtime'].append(mtime)
        if task_table['wu_id']:
            self.db.insertm('sixtrack_task', task_table)
            reviews = self.db.select('sixtrack_task',
                                     ['task_id', 'wu_id', 'last_turn'],
                                     where=f'mtime={mtime}')
            for ti, wi, lt in reviews:
                task_ids[ti] = (wi, lt)
        wu_table['task_id'] = list(task_ids.keys())
        wu_table['mtime'] = [int(time.time() * 1E7), ]*len(task_ids)
        where = {}
        where['wu_id'] = []
        where['last_turn'] = []
        for i in task_ids.values():
            where['wu_id'].append(i[0])
            where['last_turn'].append(i[1])  # wu_id is not unique now
        self.db.updatem('sixtrack_wu', wu_table, where)

        task_ids = list(task_ids.keys())
        outputs['task_id'] = task_ids
        db_info = {}
        db_info.update(self.db_info)

        tran_input = []
        if db_info['db_type'].lower() == 'sql':
            sub_name = os.path.join(self.paths['sixtrack_in'], 'sub.db')
            if os.path.exists(sub_name):
                os.remove(sub_name)  # remove the old one
            db_info['db_name'] = sub_name

            sub_db = SixDB(db_info, settings=self.db_settings, create=True)
            sub_db.create_table('preprocess_wu', self.tables['preprocess_wu'],
                                self.table_keys['preprocess_wu'])
            sub_db.create_table('preprocess_task',
                                self.tables['preprocess_task'],
                                self.table_keys['preprocess_task'])
            sub_db.create_table('sixtrack_wu_tmp', self.tables['sixtrack_wu'],
                                self.table_keys['sixtrack_wu'])
            sub_db.create_table('sixtrack_wu', self.tables['sixtrack_wu'],
                                self.table_keys['sixtrack_wu'])
            sub_db.create_table('env', self.tables['env'])
            sub_db.create_table('templates', self.tables['templates'])

            env_outs = self.db.select('env')
            names = list(self.tables['env'].keys())
            env_ins = dict(zip(names, zip(*env_outs)))
            sub_db.insertm('env', env_ins)

            temp_outs = self.db.select('templates')
            names = list(self.tables['templates'].keys())
            temp_ins = dict(zip(names, zip(*temp_outs)))
            sub_db.insertm('templates', temp_ins)

            constr = "wu_id in (%s)" % (','.join(map(str, outputs['preprocess_id'])))

            pre_outs = self.db.select('preprocess_wu', where=constr)
            names = list(self.tables['preprocess_wu'].keys())
            pre_ins = dict(zip(names, zip(*pre_outs)))
            sub_db.insertm('preprocess_wu', pre_ins)

            pre_task_ids = pre_ins['task_id']
            constr = "task_id in (%s)" % (','.join(map(str, pre_task_ids)))
            pre_task_outs = self.db.select('preprocess_task', where=constr)
            names = list(self.tables['preprocess_task'].keys())
            pre_task_ins = dict(zip(names, zip(*pre_task_outs)))
            if resubmit:
                constr = "first_turn is not null and status='submitted'"
            else:
                constr = "first_turn is not null and status='incomplete'"
            cr_ids = self.db.select('sixtrack_wu', ['wu_id', 'first_turn'],
                                    where=constr)
            if cr_ids:
                sub_db.create_table('sixtrack_task',
                                    self.tables['sixtrack_task'])
                cr_ids = list(zip(*cr_ids))
                constr = "wu_id in (%s) and last_turn in (%s)" % (
                        ','.join(map(str, cr_ids[0])),
                        ','.join(map(str, map(lambda x: x-1, cr_ids[1]))))
                cr_wu_outputs = self.db.select('sixtrack_wu', where=constr)
                if cr_wu_outputs:
                    names = list(self.tables['sixtrack_wu'].keys())
                    cr_wu_ins = dict(zip(names, zip(*cr_wu_outputs)))
                    cr_task_ids = cr_wu_ins['task_id']
                    constr = "task_id in (%s)" % (','.join(map(str,
                                                               cr_task_ids)))
                    task_outputs = self.db.select('sixtrack_task',
                                                  where=constr)
                    names = list(self.tables['sixtrack_task'].keys())
                    task_ins = dict(zip(names, zip(*task_outputs)))
                    sub_db.insertm('sixtrack_wu', cr_wu_ins)
                    sub_db.insertm('sixtrack_task', task_ins)
            sub_db.insertm('preprocess_task', pre_task_ins)
            sub_db.insertm('sixtrack_wu_tmp', outputs)
            sub_db.close()
            db_info['db_name'] = 'sub.db'
            content = "The submitted db %s is ready!" % db_info['db_name']
            self._logger.info(content)
            tran_input.append(sub_name)
        else:
            job_table = {}
            where = "task_id in (%s)" % (','.join(map(str, task_ids)))
            job_table['boinc'] = str(boinc)
            self.db.update('sixtrack_wu', job_table, where)
            self.db.create_table('sixtrack_wu_tmp', self.tables['sixtrack_wu'],
                                 self.table_keys['sixtrack_wu'])
            if not resubmit:
                self.db.insertm('sixtrack_wu_tmp', outputs)
        if boinc:
            self.init_boinc_dir()

        input_info = os.path.join(self.paths['sixtrack_in'], 'input.ini')
        self.config.clear()
        self.config.read_dict(self.sixtrack_config)
        self.config['db_info'] = db_info
        with open(input_info, 'w') as f_out:
            self.config.write(f_out)
        tran_input.append(input_info)
        in_path = self.paths['sixtrack_in']
        out_path = self.paths['sixtrack_out']
        exe = os.path.join(utils.PYSIXDESK_ABSPATH,
                           'pysixdesk/lib', 'sixtrack.py')
        if groupby:
            task_ids = self._group_records(outputs, groupby)
        self.submission.prepare(task_ids, tran_input, exe, 'input.ini',
                                in_path, out_path, flavour='tomorrow', *args,
                                **kwargs)

    def prepare_preprocess_input(self, resubmit=False, *args, **kwargs):
        '''Prepare the input files for madx and one turn sixtrack job'''
        # Prepares the preprocess config dict, in the self.preprocess_config
        # attribute.
        self._prep_preprocessing_cfg()

        self._logger.info("Preparing input files for preprocess jobs....")
        if resubmit:
            constraints = "status='submitted'"
            info = 'submitted'
        else:
            constraints = "status='incomplete'"
            info = 'incomplete'
        results = self.db.select('preprocess_wu', where=constraints)
        if not results:
            content = f"There isn't {info} preprocess job!"
            self._logger.warning(content)
            return

        names = list(self.tables['preprocess_wu'].keys())
        outputs = dict(zip(names, zip(*results)))
        wu_ids = outputs['wu_id']
        task_table = {}
        wu_table = {}
        self._logger.info("creating new lines in preprocess_task table.....")
        bar = utils.ProgressBar(len(wu_ids))
        mtime = int(time.time() * 1E7)
        task_table['wu_id'] = []
        task_table['mtime'] = []
        task_ids = OrderedDict()
        for wu_id in wu_ids:
            bar.update()
            where = f'wu_id={wu_id} and status is null'
            chck = self.db.select('preprocess_task', ['task_id'], where)
            if chck:
                task_ids[chck[0][0]] = wu_id
            else:
                task_table['wu_id'].append(wu_id)
                task_table['mtime'].append(mtime)
                # self.db.insert('preprocess_task', task_table)
                # where = "mtime=%s and wu_id=%s" % (task_table['mtime'], wu_id)
                # task_id = self.db.select('preprocess_task', ['task_id'], where)
                # task_id = task_id[0][0]
        if task_table['wu_id']:
            self.db.insertm('preprocess_task', task_table)
            where = f"mtime={mtime}"
            task_ids_new = self.db.select('preprocess_task',
                                          ['wu_id', 'task_id'], where)
            for i in task_ids_new:
                task_ids[i[1]] = i[0]
        wu_table['task_id'] = list(task_ids.keys())
        wu_table['mtime'] = [int(time.time() * 1E7)] * len(task_ids)
        where = dict([('wu_id', wu_ids)])
        self.db.updatem('preprocess_wu', wu_table, where)

        task_ids = list(task_ids.keys())
        db_info = {}
        db_info.update(self.db_info)

        trans = []
        if db_info['db_type'].lower() == 'sql':
            sub_name = os.path.join(self.paths['preprocess_in'], 'sub.db')
            if os.path.exists(sub_name):
                os.remove(sub_name)  # remove the old one
            db_info['db_name'] = sub_name
            sub_db = SixDB(db_info, settings=self.db_settings, create=True)
            sub_db.create_table('preprocess_wu', self.tables['preprocess_wu'])
            sub_db.create_table('templates', self.tables['templates'])
            temp_outs = self.db.select('templates')
            names = list(self.tables['templates'].keys())
            temp_ins = dict(zip(names, zip(*temp_outs)))
            sub_db.insertm('templates', temp_ins)
            outputs['task_id'] = task_ids
            sub_db.insertm('preprocess_wu', outputs)
            sub_db.close()
            db_info['db_name'] = 'sub.db'
            content = f"The submitted database {db_info['db_name']} is ready!"
            self._logger.info(content)
            trans.append(sub_name)

        input_info = os.path.join(self.paths['preprocess_in'], 'input.ini')
        self.config.clear()
        self.config.read_dict(self.preprocess_config)
        self.config['db_info'] = db_info
        with open(input_info, 'w') as f_out:
            self.config.write(f_out)
        trans.append(input_info)
        in_path = self.paths['preprocess_in']
        out_path = self.paths['preprocess_out']
        exe = os.path.join(utils.PYSIXDESK_ABSPATH,
                           'pysixdesk/lib', 'preprocess.py')
        self.submission.prepare(task_ids, trans, exe, 'input.ini', in_path,
                                out_path, flavour='espresso', *args, **kwargs)

    def _group_records(self, param_dict, group_key):
        """Groups a 'param_dict' based on the provided 'group_key', and
        returns nested lists of task_ids.

        Args:
            param_dict (dict): Dictionary containing lists of the study's
                parameters. Must include "task_id".
            group_key (key of param_dict): Key of "param_dict" with which to
                group the "param_dict". Must be included in "param_dict" keys.

        Returns:
            list: list of lists of task_ids.

        Raises:
            ValueError: If 'group_key' is not in 'param_dict' keys.
        """
        if group_key not in param_dict.keys():
            raise ValueError("'group_key' must be included in "
                             "'param_dict' keys.")

        param_dict_keys = list(param_dict.keys())
        group_ind = param_dict_keys.index(group_key)
        task_id_ind = param_dict_keys.index('task_id')

        # sort rows wrt to the chosen group_key
        sorted_rows = sorted(zip(*param_dict.values()),
                             key=lambda x: x[group_ind])
        task_ids = []
        # group rows wrt to the chosen group_key
        for _, g in groupby(sorted_rows,
                            key=lambda x: x[group_ind]):
            task_ids.append([i[task_id_ind] for i in g])

        return task_ids

    def prepare_cr(self):
        '''Prepare the checkpoint data, add new lines in db'''
        self._logger.info("CR feature is ON, preparing checkpoint data...")
        checks_1 = self.db.select('sixtrack_wu', ['wu_id'], DISTINCT=True)
        if checks_1:
            checks_1 = list(zip(*checks_1))[0]
        where = f"last_turn={self.last_turn}"
        checks_2 = self.db.select('sixtrack_wu', ['wu_id'], where)
        if checks_2:
            checks_2 = list(zip(*checks_2))[0]
        checks = [i for i in checks_1 if i not in checks_2]
        if not checks:
            self._logger.info(f"The tracking jobs with last turn "
                              f"{self.last_turn} already exist!")
            return True
        constraints = f"status='complete' and last_turn={self.first_turn-1}\
                and wu_id in ({','.join(map(str, checks))})"
        results = self.db.select('sixtrack_wu', where=constraints)
        if not results:
            self._logger.warning(f"There isn't complete job with last "
                                 f"turn is {self.first_turn-1}")
            return False
        N = len(results)
        names = self.tables['sixtrack_wu'].keys()
        new_lines = dict(zip(names, zip(*results)))
        new_lines['last_turn'] = (self.last_turn,)*N
        new_lines['first_turn'] = (self.first_turn,)*N
        new_lines['status'] = ('incomplete',)*N
        new_lines['turnss'] = (self.last_turn,)*N
        self.db.insertm('sixtrack_wu', new_lines)

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
