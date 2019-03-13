import os
import sys
import shutil
import inspect
import itertools
import configparser
import importlib
import utils
import mad6t_oneturn

class Study(object):

    def __init__(self, name='study', location='.'):
        '''Constructor'''
        self.name = name
        self.location = location
        self.config = configparser.ConfigParser()
        self.config.optionxform = str #preserve case
        self.mad6t_joblist = []
        #All the requested parameters for a study
        self.paths = {}
        self.madx_params = {}
        self.madx_input = {}
        self.madx_output = {}
        self.oneturn_sixtrack_params = {}
        self.oneturn_sixtrack_input = {}
        self.oneturn_sixtrack_output = {}
        self.sixtrack_params = {}
        self.sixtrack_input = {}
        self.sixtrack_output = []
        self.tables = {}
        #initialize default values
        Study.initialize(self)

    def initialize(self):
        '''initialize a study with some default settings'''
        self.paths["madx_exe"] = "/afs/cern.ch/user/m/mad/bin/madx"
        self.paths["sixtrack_exe"] = "/afs/cern.ch/project/sixtrack/build/sixtrack"
        self.paths["location"] = self.location
        self.paths["madx_in"] = os.path.join(self.location, "mad6t_input")
        self.paths["madx_out"] = os.path.join(self.location, "mad6t_output")
        self.paths["sixtrack_in"] = os.path.join(self.location, "sixtrack_input")
        self.paths["sixtrack_out"] = os.path.join(self.location, "sixtrack_output")
        app_path = os.path.abspath(inspect.getfile(Study))
        app_path = os.path.dirname(os.path.dirname(app_path))
        tem_path = os.path.join(app_path, 'templates')
        self.paths["templates"] = tem_path

        os.mkdir(self.paths["madx_in"])
        os.mkdir(self.paths["madx_out"])
        os.mkdir(self.paths["sixtrack_in"])
        os.mkdir(self.paths["sixtrack_out"])

        if os.path.isdir(tem_path):
             for item in os.listdir(tem_path):
                 s = os.path.join(tem_path, item)
                 d = os.path.join(self.location, item)
                 if os.path.isfile(s):
                     shutil.copy2(s, d)
        else:
            print("Invalid templates folder!")
        self.madx_output = {
                'fc.2': 'fort.2',
                'fc.3': 'fort.3.mad',
                'fc.3.aux': 'fort.3.aux',
                'fc.8': 'fort.8',
                'fc.16': 'fort.16',
                'fc.34': 'fort.34'}
        self.oneturn_sixtrack_params = {
                "turnss": 1,
                "nss": 1,
                "ax0s": 0.1,
                "ax1s": 0.1,
                "imc": 1,
                "iclo6": 2,
                "writebins": 1,
                "ratios": 1,
                "Runnam": 'FirstTurn',
                "idfor": 0,
                "ibtype": 0,
                "ition": 0,
                "CHRO": '/',
                "TUNE": '/',
                "POST": 'POST',
                "POS1": '',
                "ndafi": 1,
                "tunex": 62.28,
                "tuney": 60.31,
                "inttunex": 62.28,
                "inttuney": 60.31,
                "DIFF": '/DIFF',
                "DIF1": '/',
                "pmass": 938.272013,
                "emit_beam": 3.75,
                "e0": 7000,
                "bunch_charge": 1.15E11,
                "CHROM": 0,
                "chrom_eps": 0.000001,
                "dp1": 0.000001,
                "dp2": 0.000001,
                "chromx": 2,
                "chromy": 2,
                "TUNEVAL": '/',
                "CHROVAL": '/'}
        self.oneturn_sixtrack_input['input'] = self.madx_output
        self.oneturn_sixtrack_output = ['fort.10']
        self.sixtrack_output = ['fort.10']

        self.tables = {
                "mad6t_run": ['seed', 'mad_in', 'mad_out', 'fort.2', 'fort.3',\
                        'fort.8', 'fort.16', 'job_stdout', 'job_stderr',\
                        'job_stdlog', 'mad_out_mtime'],
                "six_input": list(self.sixtrack_params.keys()) + ['id_mad6t_run'],
                "six_beta": ['seed', 'tunex', 'tuney', 'beta11', 'beta12',\
                        'beta22', 'beta21', 'qx', 'qy', 'id_mad6t_run'],
                "dymap_results": []}

    def submit_mad6t(self, platform = 'local', **args):
        '''Submit the jobs to cluster or run locally'''
        clean = False
        if platform == 'local':
            if 'place' in args:
                execution_field = args['place']
            else:
                execution_field = 'temp'
            execution_field = os.path.abspath(execution_field)
            if not os.path.isdir(execution_field):
                os.mkdir(execution_field)
            if os.listdir(execution_field):
                clean = False
                print("Caution! The folder %s is not empty!"%execution_field)
            cur_path = os.getcwd()
            os.chdir(execution_field)
            if 'clean' in args:
                clean = args['clean']
            for i in self.mad6t_joblist:
                print("The job %s is running...."%i)
                mad6t_oneturn.run(i)
            print("All jobs are completed normally!")
            os.chdir(cur_path)
            if clean:
                shutil.rmtree(execution_field)
        elif platform.lower() == 'htcondor':
            #app_path = os.path.abspath(inspect.getfile(self.__class__))
            #app_path = os.path.dirname(app_path)
            #sys.path.append(app_path)
            pass
        else:
            print("Invlid platfrom!")

    def prepare_madx_single_input(self):
        '''Prepare the input files for madx and one turn sixtrack job'''
        self.config['madx'] = {}
        madx_sec = self.config['madx']
        madx_sec['source_path'] = self.paths['location']
        madx_sec['madx_exe'] = self.paths['madx_exe']
        madx_sec['mask_name'] = self.madx_input["mask_name"]
        madx_sec['output_files'] = utils.code(self.madx_output)

        self.config['mask'] = {}
        mask_sec = self.config['mask']

        self.config['sixtrack'] = {}
        six_sec = self.config['sixtrack']
        six_sec['source_path'] = self.paths['location']
        six_sec['sixtrack_exe'] = self.paths['sixtrack_exe']
        six_sec['temp_files'] = utils.code(self.oneturn_sixtrack_input['temp'])
        six_sec['input_files'] = utils.code(self.oneturn_sixtrack_input['input'])
        six_sec['output_files'] = utils.code(self.oneturn_sixtrack_output)
        self.config['fort3'] = self.oneturn_sixtrack_params

        keys = sorted(self.madx_params.keys())
        values = []
        for key in keys:
            values.append(self.madx_params[key])

        for element in itertools.product(*values):
            for i in range(len(element)):
                ky = keys[i]
                vl = element[i]
                mask_sec[ky] = str(vl)
            input_name = self.madx_name_conven('mad6t', keys, element, '.ini')
            prefix = self.madx_input['mask_name'].split('.')[0]
            madx_input_name = self.madx_name_conven(prefix, keys, element)
            madx_sec['input_name'] = madx_input_name
            out_name = self.madx_name_conven('Result', keys, element, '')
            mad6t_input = os.path.join(self.location, 'mad6t_input')
            madx_sec['dest_path'] = os.path.join(self.paths['madx_out'], out_name)
            six_sec['dest_path'] = os.path.join(self.paths['madx_out'], out_name)
            output = os.path.join(mad6t_input, input_name)
            with open(output, 'w') as f_out:
                self.config.write(f_out)
            self.mad6t_joblist.append(output)
            print('Successfully generate input file %s'%output)

    def madx_name_conven(self, prefix, keys, values, suffix = '.madx'):
        '''The convention for naming input file'''
        mk = prefix
        num = len(keys) if len(keys)<len(values) else len(values)
        for i in range(num):
            mk += '_%s_%s'%(keys[i], str(values[i]))
        mk += suffix
        return mk

class StudyFactory(object):

    def __init__(self, workspace='./sandbox'):
        self.ws = workspace
        self.studies = []
        self._setup_ws()

    def _setup_ws(self):
        '''Setup a workspace'''
        if not os.path.isdir(self.ws):
            os.mkdir(self.ws)
            print('Create new workspace %s!'%self.ws)
        else:
            print('The workspace %s already exists!'%self.ws)
        studies = os.path.join(self.ws, 'studies')
        if not os.path.isdir(studies):
            os.mkdir(studies)

    def info(self):
        '''Print all the studies in the current workspace'''
        return self.studies

    def new_study(self, name='', module='config', classname = 'MyStudy'):
        '''Create a new study'''
        self.ws = os.path.abspath(self.ws)#Get absolute path
        studies = os.path.join(self.ws, 'studies')

        if len(name) == 0:
            i = len(self.studies)
            study_name = 'study_' + str(i)
        else:
            study_name = 'study_' + name
        study = os.path.join(studies, study_name)

        if not os.path.isdir(study):
            os.mkdir(study)
            self.studies.append(study)
            mod = importlib.import_module(module)
            cls = getattr(mod, classname)
            print("Create new study %s"%study)
            return cls(name, study)
        else:
            print("The study %s already exists, nothing to do!"%study)
            sys.exit(0)
