import os
import sys
from study import Study

class MyStudy(Study):

    def __init__(self, name='study', location='.'):
        super(MyStudy, self).__init__(name, location)
        self.initialize()

    def initialize(self):
        '''initialize a study'''
        #All parameters are case-insensitive
        self.paths["madx"] = "/afs/cern.ch/user/m/mad/bin/madx"
        self.paths["sixtrack"] = "/afs/cern.ch/project/sixtrack/build/sixtrack"
        self.madx_input["mask_name"] = 'hl10.mask'
        self.madx_params = {
                "SEEDRAN": list(range(1,1+1,1)),
                "QP": list(range(1,1+1)),
                "IOCT": list(range(100,200+1,100))}
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
