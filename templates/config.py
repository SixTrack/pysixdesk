'''The template of the config file
This is a template file of preparing parameters for madx and sixtracking jobs.
'''
import os
import sys
import copy
from study import Study
from math import sqrt, pi, sin, cos

class MyStudy(Study):

    def __init__(self, name='study', location=os.getcwd()):
        super(MyStudy, self).__init__(name, location)
        '''initialize a study'''
        self.cluster_module = None #default
        self.cluster_name = 'HTCondor'
        #All parameters are case-sensitive
        #the name of mask file
        self.madx_input["mask_file"] = 'hl10.mask'
        self.madx_params["SEEDRAN"] = [1,2] #all seeds in the study
        self.madx_params["QP"] = list(range(1,1+1))#all chromaticity in the study
        self.madx_params["IOCT"] = list(range(100,200+1,100))#all octupole currents in the study
        self.oneturn_sixtrack_input['temp'] = ['fort.3.mother1', 'fort.3.mother2']
        self.oneturn_sixtrack_output = ['mychrom', 'betavalues', 'sixdesktunes']
        self.sixtrack_params = copy.deepcopy(self.oneturn_sixtrack_params)
        self.sixtrack_params['amp0'] = [8, 10]
        self.sixtrack_params['kang'] = list(range(1, 1+1))
        self.sixtrack_input['temp'] = ['fort.3.mother1', 'fort.3.mother2']
        self.sixtrack_input['input'] = copy.deepcopy(self.madx_output)

        self.env['emit'] = 3.75
        self.env['gamma'] = 7460.5
        self.env['kmax'] = 5

        #Update the user-define parameters and objects
        self.customize() #This call is mandatory

    def pre_calc(self, paramdict, pre_id):
        '''Further calculations for the specified parameters'''
        #The angle should be calculated before amplitude
        self.formulas('kang', 'angle', paramdict, pre_id)
        self.formulas('amp0', 'ax0s', paramdict, pre_id)
        #self.formulas('amp1', 'ax1s', paramdict, pre_id)

    def formulas(self, source, dest, paramdict, pre_id):
        '''The formulas for the further calculations
        @source The source parameter name
        @dest  The destination parameter name
        @paramdict The parameter dictionary, the source parameter in the dict
        will be replaced by destination parameter after calculation
        @pre_id The identified preprocess job id'''
        if source not in paramdict.keys():
            print("Invalid parameter name %s!"%source)
            sys.exit(1)
        value = paramdict.pop(source)
        try:
            value = float(value)
        except ValueError:
            print("Invalid source value for job %s!"%pre_id)
            sys.exit(1)
        except:
            print("Unexpected error!\n", traceback.print_exc())
            sys.exit(1)
        if dest == 'ax0s' or dest == 'ax1s':
            if 'angle' not in paramdict.keys():
                print("The angle should be calculated at first!")
                sys.exit(1)
            values = self.getval(pre_id, ['betax'])
            beta_x = values[0]
            kang = paramdict['angle']
            kang = float(kang)
            tt = abs(sin(pi/2*kang)/cos(pi/2*kang))
            ratio = 0.0 if tt<1.0E-15 else tt**2
            emit = self.env['emit']
            gamma = self.env['gamma']
            factor = sqrt(emit/gamma)
            ax0t = factor*(sqrt(beta_x)+sqrt(beta_x*ratio)*cos(pi/2*kang))
            value1 = ax0t*value
        elif dest == 'angle':
            kmax = self.env['kmax']
            value1 = value/(kmax+1)
        else:
            print("There isn't a formula for parameter %s!"%dest)
        paramdict[dest] = str(value1)

    def getval(self, pre_id, reqlist):
        '''Get required values from oneturn sixtrack results'''
        where = 'wu_id=%s'%pre_id
        ids = self.db.select('preprocess_wu', ['task_id'], where)
        if not ids:
            print("Wrong preprocess job id %s!"%pre_id)
            sys.exit(1)
        task_id = ids[0][0]
        if task_id is None:
            print("Incomplete preprocess job id %s!"%pre_id)
            sys.exit(1)
        where = 'task_id=%s'%task_id
        values = self.db.select('oneturn_sixtrack_result', reqlist, where)
        if not values:
            print("Wrong task id %s!"%task_id)
            sys.exit(1)
        return values[0]
