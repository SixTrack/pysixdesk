'''The template of the config file
This is a template file of preparing parameters for madx and sixtracking jobs.
'''
import os
import sys
import ast
import copy
import traceback
from study import Study
from math import sqrt, pi, sin, cos
from machineparams import MachineConfig


class MyStudy(Study):

    def __init__(self, name='study', location=os.getcwd()):
        super(MyStudy, self).__init__(name, location)
        '''initialize a study'''
        self.cluster_module = None  # default
        self.cluster_name = 'HTCondor'
        self.paths['boinc_spool'] = '/afs/cern.ch/work/b/boinc/boinctest'
        self.boinc_vars['appName'] = 'sixtracktest'
        self.env['study_type'] = 'collimation'

        ## Echo message to the terminal, if not None, echo to log_file
        self.log_file = None
        self.mes_level = 1  # message level
        ## Add database informations
        self.db_info['db_type'] = 'sql'
        # self.db_info['db_type'] = 'mysql'
        ## The follow information is needed when the db type is mysql
        # self.db_info['host'] = 'dbod-gc023'
        # self.db_info['port'] = '5500'
        # self.db_info['user'] = 'admin'
        # self.db_info['passwd'] = 'pysixdesk'

        ## Get the default values for specified machine with specified runtype
        #machine_params = MachineConfig('LHC').parameters('inj')
        machine_params = MachineConfig('LHC').parameters('col')

        ## All parameters are case-sensitive
        ## the name of mask file
        self.madx_input["mask_file"] = 'hl10.mask'
        self.madx_params["SEEDRAN"] = [1, 2]  # all seeds in the study
        # all chromaticity in the study
        self.madx_params["QP"] = list(range(1, 1 + 1))
        # all octupole currents in the study
        self.madx_params["IOCT"] = list(range(100, 200 + 1, 100))
        self.oneturn_sixtrack_input['temp'] = ['fort.3']
        self.oneturn_sixtrack_output = ['oneturnresult']
        self.oneturn_sixtrack_params.update(machine_params)
        self.oneturn_sixtrack_params['COLL'] = ''
        self.sixtrack_params = copy.deepcopy(self.oneturn_sixtrack_params)
        self.sixtrack_params['turnss'] = 1e6  # number of turns to track
        amp = [8, 10, 12]  # The amplitude
        self.sixtrack_params['amp'] = list(zip(amp, amp[1:]))  # Take pairs
        self.sixtrack_params['kang'] = list(range(1, 1 + 1))  # The angle
        self.sixtrack_input['temp'] = ['fort.3']
        self.sixtrack_input['input'] = copy.deepcopy(self.madx_output)

        ## The parameters for collimation job
        self.collimation_input['temp'] = ['fort.3']
        self.collimation_input['input'] = ['allapert.b1', 'CollDB.data',
                'SurveyWithCrossing_XP_lowb.dat', 'fc.2']
        self.collimation_output = ['aperture_losses.dat', 'coll_summary.dat']
        self.collimation_params = copy.deepcopy(self.oneturn_sixtrack_params)
        self.collimation_params['COLL'] = '/'
        self.collimation_params['turnss'] = 200
        self.collimation_params['nss'] = 5000
        self.collimation_params['ax0s'] = 0
        self.collimation_params['ax1s'] = 17
        self.collimation_params['e0'] = 6500000
        self.collimation_params['POST'] = '/'
        self.collimation_params['POS1'] = '/'
        self.collimation_params['dp2'] = 0.00
        self.collimation_params['ition'] = 1
        self.collimation_params['ibtype'] = 1
        self.collimation_params['length'] = 26658.864
        # eigen-emittances to be chosen to determine the coupling angle
        self.collimation_params['EI'] = 3.5
        # logical switch to calculate 4D(ilin=1) or DA approach 6D (ilin=2)
        self.collimation_params['ilin'] = 1

        self.env['emit'] = 3.75
        self.env['gamma'] = 7460.5
        self.env['kmax'] = 5

        ## Update the user-define parameters and objects
        self.customize()  ## This call is mandatory

    def pre_calc(self, paramdict, pre_id):
        '''Further calculations for the specified parameters'''
        # The angle should be calculated before amplitude
        status = []
        status.append(self.formulas('kang', 'angle', paramdict, pre_id))
        status.append(self.formulas(
            'amp', ['ax0s', 'ax1s'], paramdict, pre_id))
        return all(status)

    def formulas(self, source, dest, paramdict, pre_id):
        '''The formulas for the further calculations,
        this function should be customized by the user!
        @source The source parameter name
        @dest  The destination parameter name
        @paramdict The parameter dictionary, the source parameter in the dict
        will be replaced by destination parameter after calculation
        @pre_id The identified preprocess job id
        @return The status'''
        if source not in paramdict.keys():
            print("Invalid parameter name %s!" % source)
            return 0
        value = paramdict.pop(source)
        try:
            value = ast.literal_eval(value)
        except ValueError:
            print("Invalid source value for job %s!" % pre_id)
            return 0
        except:
            print("Unexpected error!\n", traceback.print_exc())
            return 0
        if source == 'amp':
            if 'angle' not in paramdict.keys():
                print("The angle should be calculated before amplitude!")
                return 0
            try:
                values = self.getval(pre_id, ['betax'])
                beta_x = values[0]
                kang = paramdict['angle']
                kang = float(kang)
                tt = abs(sin(pi / 2 * kang) / cos(pi / 2 * kang))
                ratio = 0.0 if tt < 1.0E-15 else tt**2
                emit = self.env['emit']
                gamma = self.env['gamma']
                factor = sqrt(emit / gamma)
                ax0t = factor * (sqrt(beta_x) + sqrt(beta_x * ratio) * cos(pi / 2 * kang))
                value0 = ax0t * value[0]
                value1 = ax0t * value[1]
                paramdict[dest[0]] = str(value0)
                paramdict[dest[1]] = str(value1)
                return 1
            except:
                print("Unexpected error!\n", traceback.print_exc())
                return 0
        elif source == 'kang':
            try:
                kmax = self.env['kmax']
                value1 = value / (kmax + 1)
                paramdict[dest] = str(value1)
                return 1
            except:
                print("Unexpected error!\n", traceback.print_exc())
                return 0
        else:
            print("There isn't a formula for parameter %s!" % dest)
            return 0

    def getval(self, pre_id, reqlist):
        '''Get required values from oneturn sixtrack results'''
        where = 'wu_id=%s' % pre_id
        ids = self.db.select('preprocess_wu', ['task_id'], where)
        if not ids:
            print("Wrong preprocess job id %s!" % pre_id)
            sys.exit(1)
        task_id = ids[0][0]
        if task_id is None:
            print("Incomplete preprocess job id %s!" % pre_id)
            sys.exit(1)
        where = 'task_id=%s' % task_id
        values = self.db.select('oneturn_sixtrack_result', reqlist, where)
        if not values:
            print("Wrong task id %s!" % task_id)
            sys.exit(1)
        return values[0]
