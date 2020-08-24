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
from pysixdesk.lib import utils

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

        # Need to use more recent release of Sixtrack than what is defined in PySixDesk by default.
        self.paths['sixtrack_exe'] = ('/afs/cern.ch/project/sixtrack/build/latest/' + 
            'SixTrack_50403-9e43f07_zlib_crlibm_rn_Linux_gfortran_static_avx2_x86_64_64bit_double')

        # Database type
        self.db_info['db_type'] = 'sql'
        # self.db_info['db_type'] = 'mysql'

        self.oneturn = False  # Switch for oneturn sixtrack job
        # self.collimation = True

        # All parameters are case-sensitive
        # the name of mask file
        mask_file = 'example.mask'
        self.madx_input["mask_file"] = mask_file
        self.madx_output = {'fc.2': 'fort.2',
                            'fc.3': 'fort.3.mad',
                            'fc.3.aux': 'fort.3.aux',
                            'fc.8': 'fort.8',
                            'fc.16': 'fort.16',
                            'fc.34': 'fort.34'}

        # All parameters are case-sensitive
        self.params = StudyParams(mask_path=os.path.join(self.study_path, mask_file),
                                  fort_path=os.path.join(self.study_path, 'fort.3'),
                                  machine_defaults=machineparams.LHC['inj'])
        self.params['Runnam'] = name
        self.params['turnss'] = int(500)  # number of turns to track (must be int)


        # Particle initialization
        self.params['dist_type'] = 'cartesian'  # type of particle distribution (cartesian or polar)
        amp = [float(i) for i in range(5, 16, 1)]

        if self.params['dist_type'] == 'polar':
            self.params['phase_space_var1'] = list(zip(amp, amp[1:]))  # Take pairs # amp_x
            self.params['phase_space_var2'] = utils.linspace(0, pi/2., 12)[1:-1] # angles
            n_particles_per_amplitude_angle_pair = 10
            self.params['nss'] = 2 * n_particles_per_amplitude_angle_pair  # account for twin particles
        elif self.params['dist_type'] == 'cartesian':
            self.params['phase_space_var1'] = list(zip(amp, amp[1:]))  # Take pairs # amp_x
            self.params['phase_space_var2'] = list(zip(amp, amp[1:]))  # Take pairs # amp_y
            n_particles_per_amp_pair = 10
            self.params['nss'] = 2 * n_particles_per_amp_pair**2  # account for twin particles
        else:
            raise ValueError('Unknown dist_type')

        self.params['seed_ran'] = 10
        self.params['sixtrack_seed'] = 42

        self.params['i_mo'] = -10.
        self.params['tune_x'] = 62.28
        self.params['tune_y'] = 60.31
        self.params['int_tune_x'] = 62
        self.params['int_tune_y'] = 60

        q_prime = 20.
        self.params['q_prime'] = q_prime
        self.params['q_prime_x'] = q_prime
        self.params['q_prime_y'] = q_prime

        self.params['sig_e'] = 4.5e-4
        self.params['sig_z'] = 0.11
        self.params['bunch_charge'] = 1e11
        self.params['emit_norm'] = 3.5
        self.params['b_t_dist'] = 25.
        self.params['e_0'] = 450000.
        self.params['rf_vol'] = 8.

        # @set_input_keys(['angle', 'amp'])
        # @set_output_keys(['x0', 'x1', 'y0', 'y1'])
        # def calc_dist_coords(angle, amp):
        #     x0, x1 = cos(angle)*amp[0], cos(angle)*amp[1]
        #     y0, y1 = sin(angle)*amp[0], sin(angle)*amp[1]
        #     return x0, x1, y0, y1
        # self.params.calc_queue.append(calc_dist_coords)

        #     return angles
        # self.params.calc_queue.append(set_angles)

        # @set_input_keys(['kang', 'kmax'])
        # @set_output_keys(['angle'])
        # def calc_angle(kang, kmax):
        #     return kang / (kmax + 1)  # 1-->pi/2, 0.5-->pi/4, ...
        # self.params.calc_queue.append(calc_angle)

        # @set_input_keys(['angle'])
        # @set_output_keys(['ratio'])
        # def calc_ratio(angle):
        #     ratio = abs(tan((pi / 2) * angle))
        #     if ratio < 1e-15:
        #         ratio = 0.
        #     else:
        #         ratio = ratio ** 2
        #     return ratio
        # self.params.calc_queue.append(calc_ratio)

        # # should it not be betax and betax2 ?
        # @set_requirements({'oneturn_sixtrack_results': ['betax', 'betax2']})
        # @set_input_keys(['angle', 'ratio', 'emit_norm_x', 'e_0', 'pmass', 'amp'])
        # @set_output_keys(['ax0s', 'ax1s'])
        # def calc_amp(angle, ratio, emit, e_0, pmass, amp, betax=None, betax2=None):
        #     gamma = e_0 / pmass
        #     factor = sqrt(emit / gamma)
        #     ax0t = factor * (sqrt(betax) + sqrt(betax2 * ratio) * cos((pi / 2) * angle))
        #     return amp[0] * ax0t, amp[1] * ax0t
        # self.params.calc_queue.append(calc_amp)

        self.oneturn_sixtrack_input['fort_file'] = 'fort.3'
        self.oneturn_sixtrack_output = ['oneturnresult']
        self.sixtrack_input['fort_file'] = 'fort.3'
        self.preprocess_output = dict(self.madx_output)
        self.sixtrack_input['input'] = dict(self.preprocess_output)
        self.sixtrack_input['additional_input'] = ['init_cartesian_dist.py', 'init_polar_dist.py']

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
