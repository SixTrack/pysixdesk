'''The template of the config file
This is a template file of preparing parameters for madx and sixtracking jobs.
'''
import os
import logging

from collections import OrderedDict

from pysixdesk.lib import submission
from pysixdesk import Study
from pysixdesk.lib.study_params import StudyParams
from pysixdesk.lib.study_params import set_input_keys
from pysixdesk.lib.study_params import set_requirements
from pysixdesk.lib.study_params import set_output_keys
from math import sqrt, pi, sin, cos, tan
from pysixdesk.lib import machineparams

# logger configuration
logger = logging.getLogger('pysixdesk')
logger.setLevel(logging.INFO)

# To add logging to file, do:
# -----------------------------------------------------------------------------
study_path = os.path.dirname(__file__)
log_path = os.path.join(study_path, 'pysixdesk.log')
filehandler = logging.FileHandler(log_path)
fmt = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s',
                        datefmt='%b/%d %H:%M:%S')
filehandler.setFormatter(fmt)
filehandler.setLevel(logging.DEBUG)
logger.addHandler(filehandler)
# -----------------------------------------------------------------------------


class MyStudy(Study):

    def __init__(self, name='study', location=os.getcwd()):
        super(MyStudy, self).__init__(name, location)
        '''initialize a study'''
        self.cluster_class = submission.HTCondor
        self.paths['boinc_spool'] = '/afs/cern.ch/work/b/boinc/boinctest'
        self.boinc_vars['appName'] = 'sixtracktest'
        # apt exe paths
        self.paths['madx_exe'] = '/afs/cern.ch/work/l/lcoyle/public/pysixdesk_testing/pysixdesk_params/madx-apt'
        self.paths['sixtrack_exe'] = '/afs/cern.ch/work/l/lcoyle/public/pysixdesk_testing/pysixdesk_params/sixtrack-apt'

        # Database type
        self.db_info['db_type'] = 'sql'
        # self.db_info['db_type'] = 'mysql'

        self.oneturn = False  # Switch for oneturn sixtrack job
        # self.collimation = True

        # All parameters are case-sensitive
        # the name of mask file
        mask_file = 'job_aper_michael.madx'
        fort_file = 'fort.3_michael'
        self.madx_input["mask_file"] = mask_file
        self.madx_output = {
            'fc.2': 'fort.2',
            'fc.3': 'fort.3.mad',
            'fc.3.aper': 'fort.3.aper',
            'fc.3.aux': 'fort.3.aux',
            'fc.8': 'fort.8',
            'fc.16': 'fort.16',
            'fc.34': 'fort.34'}
        # All parameters are case-sensitive
        self.params = StudyParams(mask_path=os.path.join(self.study_path, mask_file),
                                  fort_path=os.path.join(self.study_path, fort_file),
                                  machine_defaults=machineparams.HLLHC['inj'])
        self.params['Runnam'] = name
        amp = [8, 10, 12]
        self.params['turnss'] = int(1)  # number of turns to track (must be int)
        self.params['nss'] = 30
        self.params['amp'] = list(zip(amp, amp[1:]))  # Take pairs
        self.params['angle'] = self.params.da_angles(start=0, end=pi/2, n=3)

        self.params['seed_ran'] = 1
        self.params['i_mo'] = [-6.5, -13]  # list(range(100, 200 + 1, 100))
        self.params['sixtrack_seed'] = 42
        self.params['tune_x'] = 62.28
        self.params['tune_y'] = 60.31
        self.params['int_tune_x'] = int(self.params['tune_x'])
        self.params['int_tune_y'] = int(self.params['tune_y'])
        self.params['emit_norm_x'] = 3.5
        self.params['emit_norm_y'] = 3.5
        self.params['rf_vol'] = 8

        # TODO: UGLY HACK TO ADD JUST THE apertue_losses file...
        self.tables['aperture_losses'] = OrderedDict([
            ('task_id', 'int'),
            ('row_num', 'int'),
            ('turn', 'int'),
            ('block', 'int'),
            ('bezid', 'int'),
            ('bez', 'text'),
            ('slos', 'float'),
            ('part_id', 'int'),
            ('x', 'float'),
            ('xp', 'float'),
            ('y', 'float'),
            ('yp', 'float'),
            ('etot', 'float'),
            ('dE', 'float'),
            ('dT', 'float'),
            ('A_atom', 'int'),
            ('Z_atom', 'int'),
            ('mtime', 'bigint')])
        self.table_keys['aperture_losses'] = {
            'primary': ['task_id', 'row_num'],
            'foreign': {'sixtrack_task': [['task_id'], ['task_id']]},
        }

        self.oneturn_sixtrack_input['fort_file'] = fort_file
        self.oneturn_sixtrack_output = ['oneturnresult']
        self.sixtrack_input['fort_file'] = fort_file
        self.preprocess_output = dict(self.madx_output)
        self.sixtrack_input['input'] = dict(self.preprocess_output)
        self.sixtrack_input['additional_input'] = ['init_dist.py']
        self.sixtrack_output = ['fort.10', 'sixtrack_aper', 'aperture_losses.dat']

        # # The parameters for collimation job
        # self.madx_output = {
        #     'fc.2': 'fort.2',
        #     'fc.3': 'fort.3.mad',
        #     'fc.3.aux': 'fort.3.aux',
        #     'fc.8': 'fort.8'}
        # self.collimation_input = {'aperture':'allapert.b1',
        #         'survey':'SurveyWithCrossing_XP_lowb.dat'}
        # self.oneturn_sixtrack_input['input'] = dict(self.madx_output)
        # self.preprocess_output = dict(self.madx_output)
        # self.sixtrack_input['fort_file'] = 'fort.3'
        # self.sixtrack_input['input'] = self.preprocess_output
        # self.sixtrack_input['additional_input'] = ['CollDB.data']
        # self.sixtrack_output = ['aperture_losses.dat', 'coll_summary.dat']
        # self.params['toggle_coll'] = '/'
        # self.params['turnss'] = 200
        # self.params['nss'] = 5000
        # self.params['ax0s'] = 0
        # self.params['ax1s'] = 17
        # self.params['e_0'] = 6500000
        # self.params['toggle_post'] = '/'
        # self.params['dp2'] = 0.00
        # self.params['ition'] = 1
        # self.params['ibtype'] = 1
        # self.params['length'] = 26658.864
        # # eigen-emittances to be chosen to determine the coupling angle
        # self.params['EI'] = 3.5
        # # logical switch to calculate 4D(ilin=1) or DA approach 6D (ilin=2)
        # self.params['ilin'] = 1

        # Update the user-define parameters and objects
        self.customize()  # This call is mandatory
