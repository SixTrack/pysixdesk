import os
import sys
from study import Study

class MyStudy(Study):

    def __init__(self, name = 'study'):
        Study.__init__(self, name)

    def initial(self):
        '''initialize a study'''
        #All parameters are case-insensitive
        self.paths["madx"] = "/afs/cern.ch/user/m/mad/bin/madx"
        self.paths["sixtrack"] = "/afs/cern.ch/project/sixtrack/build/sixtrack"
        self.madx_input = ['hl10.mask']
        self.madx_params = {
                "SEEDRAN": list(range(1,1+1,1)),
                "QP": list(range(1,1+1)),
                "IOCT": list(range(100,200+1,100))}
        self.oneturn_sixtrack_input = ['fort.3.mother1', 'fort.3.mother2']
        self.oneturn_sixtrack_output = ['mychrom', 'betavalues', 'sixdesktunes']
        self.sixtrack_parms = {
                "turnss": [],
                "amplitude": [],
                "angle": [],
                "chromx": [],
                "chromy": [],
                "tunex": [],
                "tuney": [],}
        self.sixtrack_output_files = ['fort.10']
        tables = {
                "mad6t_run": ['seed', 'mad_in', 'mad_out', 'job_stdout',\
                        'job_stderr', 'job_stdlog', 'mad_out_mtime'],
                "six_input": list(self.sixtrack_parms.keys()) +\
                        ['fort.2', 'fort.8', 'fort.16'],
                "six_beta": ['seed', 'tunex', 'tuney', 'beta11', 'beta12',\
                        'beta22', 'beta21', 'qx', 'qy','id_mad6t_run'],
                "dymap_results": []}
        self.tables.update(tables)

if __name__ == '__main__':
    s = MyStudy()
    s.initial()
