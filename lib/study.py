import os
import sys
import math
import shutil
import inspect
import itertools
import configparser
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
        self.madx_input = {}
        self.madx_output = {}
        self.sixtrack_oneturn_params = {}
        self.sixtrack_input = []
        self.sixtrack_output = []
        self.tables = {}
        self.vals = {}
        self.mvals = {}
        self.initial()

    def initial(self):
        '''initialize a study with some default settings'''
        self.paths["sixtrack"] = "/afs/cern.ch/project/sixtrack/build/sixtrack"
        self.paths["madx"] = "/afs/cern.ch/user/m/mad/bin/madx"
        self.paths["location"] = self.location
        self.paths["madx_in"] = os.path.join(self.location, "mad6t_input")
        self.paths["madx_out"] = os.path.join(self.location, "mad6t_output")
        self.paths["sixtrack_in"] = os.path.join(self.location, "sixtrack_input")
        self.paths["sixtrack_out"] = os.path.join(self.location, "sixtrack_output")

        self.madx_output = {
                'fc.2': 'fort.2',
                'fc.3.aux': 'fort.3.aux',
                'fc.8': 'fort.8',
                'fc.16': 'fort.16'}
        self.sixtrack_oneturn_params = {
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
        self.sixtrack_output_files = ['fort.10']

        self.tables = {
                "mad6t_run": ['seed', 'mad_in', 'mad_out', 'fort.2', 'fort.3',\
                        'fort.8', 'fort.16', 'job_stdout', 'job_stderr',\
                        'job_stdlog', 'mad_out_mtime'],
                "six_input": list(self.sixtrack_parms.keys()) + ['id_mad6t_run'],
                "six_beta": ['seed', 'tunex', 'tuney', 'beta11', 'beta12',\
                        'beta22', 'beta21', 'qx', 'qy', 'id_mad6t_run'],
                "dymap_results": []}

    def from_env_file(self, mfile, *files):
        '''Set up a study from the initial files of old version sixdesk, e.g. sixdeskenv,sixenv
        '''
        self.mvals = self.parse_bash_script(mfile)
        if os.path.isfile('cob_env.sh'):
            os.remove('cob_env.sh')
        f_out = open('cob_env.sh', 'w')
        for a in files:
            if os.path.isfile(a):
                f_in = open(a, 'r')
                f_lines = f_in.readlines()
                f_in.close()
                f_out.writelines(f_lines)
            else:
                print("The input file %s doesn't exist!"%a)
        f_out.close()
        val1 = self.parse_bash_script('cob_env.sh')
        os.remove('cob_env.sh')
        self.vals.update(val1)

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

    def prepare_mad6t_input(self):
        '''Prepare the input files for madx and one turn sixtrack job'''
        #parameters for madx job
        basefile = os.path.join(self.location, 'mad6t.ini')#hard code?
        self.config.read(basefile)
        madx_sec = self.config['madx']
        madx_sec['source_path'] = self.location
        madx_path = self.vals['MADX_PATH']
        madx_exe = self.vals['MADX']
        madx_sec['madx_exe'] = os.path.join(madx_path, madx_exe)
        seed_i = self.vals['istamad']
        seed_e = self.vals['iendmad']
        madx_sec['job_name'] = self.vals['LHCDescrip']
        madx_sec['corr_test'] = self.vals['CORR_TEST']
        madx_sec['fort_34'] = self.vals['fort_34']
        scan_vars = self.mvals['scan_variables'].split()
        scan_hols = self.mvals['scan_placeholders'].replace('%','').split()
        scan_vals = []
        for a in scan_vars:
            val = self.mvals['scan_vals_'+a]
            val = val.split()
            val = [num(i) for i in val if not math.isnan(num(i))]
            if len(val) == 1:
                val = [k+1 for k in range(int(val[0]))]
            scan_vals.append(val)
        s_i = num(seed_i)
        s_e = num(seed_e)
        seeds = [j+s_i for j in range(int(s_e-s_i+1))]
        scan_hols.append('SEEDRAN')
        scan_vals.append(seeds)

        mask_sec = self.config['mask']

        #parameters for one turn sixtrack job
        six_sec = self.config['sixtrack']
        six_sec['source_path'] = self.location
        fort3_sec = self.config['fort3']
        fort3_sec['tunex'] = self.vals['tunex']
        fort3_sec['tuney'] = self.vals['tuney']
        fort3_sec['inttunex'] = self.vals['tunex']
        fort3_sec['inttuney'] = self.vals['tuney']
        fort3_sec['pmass'] = self.vals['pmass']
        fort3_sec['emit_beam'] = self.vals['emit_beam']
        fort3_sec['e0'] = self.vals['e0']
        fort3_sec['bunch_charge'] = self.vals['bunch_charge']
        fort3_sec['chrom_eps'] = self.vals['chrom_eps']
        fort3_sec['CHROM'] = self.vals['chrom']
        fort3_sec['chromx'] = self.vals['chromx']
        fort3_sec['chromy'] = self.vals['chromy']

        for element in itertools.product(*scan_vals):
            input_name = 'mad6t'
            out_name = 'result'
            for i in range(len(element)):
                ky = scan_hols[i]
                vl = element[i]
                mask_sec[ky] = str(vl)
                input_name = input_name + '_' + str(ky) + '_' + str(vl)
                out_name = out_name + '_' + str(ky) + '_' + str(vl)
            input_name = input_name + '.ini'
            mad6t_input = os.path.join(self.location, 'mad6t_input')
            madx_sec['dest_path'] = os.path.join(self.location, \
                                    'mad6t_output', out_name)
            six_sec['dest_path'] = os.path.join(self.location, \
                                    'mad6t_output', out_name)
            output = os.path.join(mad6t_input, input_name)
            with open(output, 'w') as f_out:
                self.config.write(f_out)
            self.mad6t_joblist.append(output)
            print('Successfully generate input file %s'%output)

    def parse_bash_script(self, mfile):
        '''parse the bash input file for the old version sixdesk'''
        mf_in = open(mfile, 'r')
        mf_lines = mf_in.readlines()
        mf_in.close()
        if os.path.isfile('mexe.sh'):
            os.remove('mexe.sh')
        mf_exe = open('mexe.sh', 'w')
        mf_exe.write('#!/bin/bash\n')
        coms = []
        params = []
        for line in mf_lines:
            if '#' not in line:
                mf_exe.write(line)
                if 'export' in line:
                    m_ar = line.split()
                    if len(m_ar) > 1:
                        m_ar1 = m_ar[1].split('=')
                        param = m_ar1[0]
                        param = peel_str(param)
                        if param not in params:
                            command = 'echo $' + param + '\n'
                            coms.append(command)
                            params.append(param)
                elif '=' in line and 'if' not in line:
                #There is a potential bug, 
                #when the parameter name contains 'if' it will be ignored
                    m_ar = line.split('=')
                    param = m_ar[0]
                    param = peel_str(param)
                    if param not in params:
                        command = 'echo $' + param + '\n'
                        coms.append(command)
                        params.append(param)
                else:
                    pass
        mf_exe.writelines(coms)
        mf_exe.close()
        os.chmod('mexe.sh', 0o777)
        values = os.popen('./mexe.sh').readlines()
        vals = {}
        os.remove('mexe.sh')
        if len(params) == len(values):
            for i in range(len(params)):
                vals[params[i]] = values[i].replace('\n', '')
        else:
            print("There should be something wrong with your input files!")
        return vals

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

    def new_study(self, name='', templates = ''):
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
            os.mkdir(os.path.join(study, 'mad6t_input'))
            os.mkdir(os.path.join(study, 'mad6t_output'))
            os.mkdir(os.path.join(study, 'sixtrack_input'))
            os.mkdir(os.path.join(study, 'sixtrack_output'))
            if len(templates) == 0:
                app_path = os.path.abspath(inspect.getfile(self.__class__))
                app_path = os.path.dirname(app_path)
                app_path = os.path.dirname(app_path)
                tem_path = os.path.join(app_path, 'templates')
                templates = tem_path

            if os.path.isdir(templates):
                for item in os.listdir(templates):
                    s = os.path.join(templates, item)
                    d = os.path.join(study, item)
                    if os.path.isfile(s):
                        shutil.copy2(s, d)
            else:
                print("Invlid templates source path!")
            self.studies.append(study)
            print("Create new study %s"%study)
            return Study(name, study)
        else:
            print("The study %s already exists, nothing to do!"%study)
            sys.exit(0)

def peel_str(val, query=['(',')','\n',' '], replace=['','','','']):
    for que,rep in zip(query, replace):
        val = val.replace(que, rep)
    return val

def num(val):
    if is_numeral(val):
        try:
            return int(val)
        except valueError:
            return float(val)
    else:
        return float('nan')

def is_numeral(val):
    try:
        float(val)
        return True
    except valueError:
        return False
