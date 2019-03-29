'''The template of the config file
This is a template file of preparing parameters for madx and sixtracking jobs.
'''
import os
import sys
import copy
from study import Study

class MyStudy(Study):

    def __init__(self, name='study', location=os.getcwd()):
        super(MyStudy, self).__init__(name, location)
        '''initialize a study'''
        #All parameters are case-sensitive
        #the name of mask file
        self.madx_input["mask_name"] = 'hl10.mask'
        self.madx_params = {
                "SEEDRAN": [1],#all seeds in the study
                "QP": list(range(1,1+1)),#all chromaticity in the study
                "IOCT": list(range(100,200+1,100))#all octupole currents in the study
                }
        self.oneturn_sixtrack_input['temp'] = ['fort.3.mother1', 'fort.3.mother2']
        self.oneturn_sixtrack_output = ['mychrom', 'betavalues', 'sixdesktunes']
        self.sixtrack_params = copy.deepcopy(self.oneturn_sixtrack_params)
        self.sixtrack_params['ax0s'] = [0.1, 0.2]
        self.sixtrack_input['temp'] = ['fort.3.mother1', 'fort.3.mother2']
        self.sixtrack_input['input'] = copy.deepcopy(self.madx_output)

        #Add the user-defined scan parameters and outputs into database
        self.update_tables()
