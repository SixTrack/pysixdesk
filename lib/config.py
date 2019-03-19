import os
import sys
from study import Study

class MyStudy(Study):

    def __init__(self, name='study', location=os.getcwd(), parent_ws=None):
        super(MyStudy, self).__init__(name, location, parent_ws)
        '''initialize a study'''
        #All parameters are case-sensitive
        #full path to madx
        self.paths["madx"] = "/afs/cern.ch/user/m/mad/bin/madx"
        #full path to sixtrack
        self.paths["sixtrack"] = "/afs/cern.ch/project/sixtrack/build/sixtrack"
        #the name of mask file
        self.madx_input["mask_name"] = 'hl10.mask'
        self.madx_params = {
                "SEEDRAN": [1],#all seeds in the study
                "QP": list(range(1,1+1)),#all chromaticity in the study
                "IOCT": list(range(100,200+1,100))#all octupole currents in the study
                }
        self.oneturn_sixtrack_input['temp'] = ['fort.3.mother1', 'fort.3.mother2']
        self.oneturn_sixtrack_output = ['mychrom', 'betavalues', 'sixdesktunes']
        self.sixtrack_params = {
                "turnss": [],
                "amplitude": [],
                "angle": [],
                "chromx": [],
                "chromy": [],
                "tunex": [],
                "tuney": [],}
        self.sixtrack_input = []
