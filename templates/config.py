'''The template of the config file
This is a template file of preparing parameters for madx and sixtracking jobs.
'''
import os
import logging

from pysixdesk.lib import submission
from pysixdesk import Study
from pysixdesk.lib.study_params import StudyParams, set_property
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

        # Database type
        self.db_info['db_type'] = 'sql'
        # self.db_info['db_type'] = 'mysql'

        self.oneturn = True  # Switch for oneturn sixtrack job
        # self.collimation = True

        # All parameters are case-sensitive
        # the name of mask file
        mask_file = 'hl10.mask'
        self.madx_input["mask_file"] = mask_file
        # All parameters are case-sensitive
        self.params = StudyParams(mask_path=os.path.join(self.study_path, mask_file),
                                  fort_path=os.path.join(self.study_path, 'fort.3'),
                                  machine_defaults=machineparams.HLLHC['col'])
        self.params['Runnam'] = name
        amp = [8, 10, 12]
        self.params['turnss'] = int(1e2)  # number of turns to track (must be int)
        self.params['nss'] = 30
        self.params['amp'] = list(zip(amp, amp[1:]))  # Take pairs
        self.params['kang'] = list(range(1, 1 + 2))  # The angle
        self.params['kmax'] = 5
        # self.params['toggle_coll/'] = '/'
        self.params['SEEDRAN'] = [1, 2]
        self.params['I_MO'] = list(range(100, 200 + 1, 100))

        @set_property('input_keys', ['kang', 'kmax'])
        @set_property('output_keys', ['angle'])
        def calc_angle(kang, kmax):
            return kang / (kmax + 1)  # 1-->pi/2, 0.5-->pi/4, ...
        self.params.calc_queue.append(calc_angle)

        @set_property('input_keys', ['angle'])
        @set_property('output_keys', ['ratio'])
        def calc_ratio(angle):
            ratio = abs(tan((pi / 2) * angle))
            if ratio < 1e-15:
                ratio = 0.
            else:
                ratio = ratio ** 2
            return ratio
        self.params.calc_queue.append(calc_ratio)

        # should it not be betax and betax2 ?
        @set_property('require', {'oneturn_sixtrack_results': ['betax', 'betax2']})
        @set_property('input_keys', ['angle', 'ratio', 'emit_norm_x', 'e0', 'pmass', 'amp'])
        @set_property('output_keys', ['ax0s', 'ax1s'])
        def calc_amp(angle, ratio, emit, e0, pmass, amp, betax=None, betax2=None):
            gamma = e0 / pmass
            factor = sqrt(emit / gamma)
            ax0t = factor * (sqrt(betax) + sqrt(betax2 * ratio) * cos((pi / 2) * angle))
            return amp[0] * ax0t, amp[1] * ax0t
        self.params.calc_queue.append(calc_amp)

        self.oneturn_sixtrack_input['fort_file'] = 'fort.3'
        self.oneturn_sixtrack_output = ['oneturnresult']
        self.sixtrack_input['fort_file'] = 'fort.3'
        self.preprocess_output = dict(self.madx_output)
        self.sixtrack_input['input'] = dict(self.preprocess_output)

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
        # self.sixtrack_input['temp'] = 'fort.3'
        # self.sixtrack_input['input'] = self.preprocess_output
        # self.sixtrack_input['additional_input'] = ['CollDB.data']
        # self.sixtrack_output = ['aperture_losses.dat', 'coll_summary.dat']
        # self.params['toggle_coll'] = '/'
        # self.params['turnss'] = 200
        # self.params['nss'] = 5000
        # self.params['ax0s'] = 0
        # self.params['ax1s'] = 17
        # self.params['e0'] = 6500000
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
