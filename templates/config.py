'''The template of the config file
This is a template file of preparing parameters for madx and sixtracking jobs.
'''
import os
import copy
import logging

from pysixdesk.lib import submission
from pysixdesk import Study
from pysixdesk.lib.study_params import StudyParams, set_property
from math import sqrt, pi, sin, cos
from pysixdesk.lib import machineparams

# logger configuration
logger = logging.getLogger('pysixdesk')
logger.setLevel(logging.INFO)

# To add logging to file, do:
# -----------------------------------------------------------------------------
# filehandler = logging.FileHandler(log_path)
# filehandler.setFormatter(logging.Formatter(format='%(asctime)s %(name)s %(levelname)s: %(message)s',
#                                            datefmt='%H:%M:%S'))
# filehandler.setLevel(logging.DEBUG)
# logger.addHandler(filehandler)
# -----------------------------------------------------------------------------


class MyStudy(Study):

    def __init__(self, name='study', location=os.getcwd()):
        super(MyStudy, self).__init__(name, location)
        '''initialize a study'''
        self.cluster_class = submission.HTCondor
        self.paths['boinc_spool'] = '/afs/cern.ch/work/b/boinc/boinctest'
        self.boinc_vars['appName'] = 'sixtracktest'

        # Add database informations
        self.db_info['db_type'] = 'sql'
        # self.db_info['db_type'] = 'mysql'
        # The follow information is needed when the db type is mysql
        # self.db_info['host'] = 'dbod-gc023'
        # self.db_info['port'] = '5500'
        # self.db_info['user'] = 'admin'
        # self.db_info['passwd'] = 'pysixdesk'

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
        self.params['turnss'] = int(1e3)
        self.params['amp'] = list(zip(amp, amp[1:]))  # Take pairs
        self.params['kang'] = list(range(1, 1 + 2))  # The angle
        self.params['kmax'] = 5
        self.params['toggle_coll/'] = ''

        @set_property('input_keys', ['kang', 'kmax'])
        @set_property('output_keys', ['angle'])
        def calc_angle(kang, kmax):
            return kang / (kmax + 1)
        self.params.calc_queue.append(calc_angle)

        @set_property('require', {'oneturn_sixtrack_result': 'betax'})
        @set_property('input_keys', ['angle', 'emit_norm_x', 'e0', 'pmass', 'amp'])
        @set_property('output_keys', ['ax0s', 'ax1s'])
        def calc_amp(angle, emit, e0, pmass, amp, betax=None):
            tt = abs(sin(pi / 2 * angle) / cos(pi / 2 * angle))
            ratio = 0.0 if tt < 1.0E-15 else tt**2
            gamma = e0 / pmass
            factor = sqrt(emit / gamma)
            ax0t = factor * (sqrt(betax) + sqrt(betax * ratio) * cos(pi / 2 * angle))
            return amp[0] * ax0t, amp[1] * ax0t
        self.params.calc_queue.append(calc_amp)

        self.oneturn_sixtrack_input['temp'] = ['fort.3']
        self.oneturn_sixtrack_output = ['oneturnresult']
        self.sixtrack_input['temp'] = ['fort.3']
        self.preprocess_output = copy.deepcopy(self.madx_output)
        self.sixtrack_input['input'] = self.preprocess_output

        ## The parameters for collimation job
        # self.madx_output = {
        #     'fc.2': 'fort.2',
        #     'fc.3': 'fort.3.mad',
        #     'fc.3.aux': 'fort.3.aux',
        #     'fc.8': 'fort.8'}
        # self.collimation_input = {'aperture':'allapert.b1',
        #         'survey':'SurveyWithCrossing_XP_lowb.dat'}
        # self.preprocess_output = copy.deepcopy(self.madx_output)
        # self.sixtrack_input['temp'] = ['fort.3']
        # self.sixtrack_input['input'] = self.preprocess_output
        # self.sixtrack_input['additional_input'] = ['CollDB.data']
        # self.sixtrack_output = ['aperture_losses.dat', 'coll_summary.dat']
        # self.params['COLL'] = '/'
        # self.params['turnss'] = 200
        # self.params['nss'] = 5000
        # self.params['ax0s'] = 0
        # self.params['ax1s'] = 17
        # self.params['e0'] = 6500000
        # self.params['POST'] = '/'
        # self.params['POS1'] = '/'
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
