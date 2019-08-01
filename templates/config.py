'''The template of the config file
This is a template file of preparing parameters for madx and sixtracking jobs.
'''
import os
import copy
import logging

from pysixdesk.lib import submission
from pysixdesk import Study
from pysixdesk.lib.study_params import StudyParams
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
        # self.db_info['db_type'] = 'sql'
        self.db_info['db_type'] = 'mysql'
        # The follow information is needed when the db type is mysql
        self.db_info['host'] = 'dbod-gc023'
        self.db_info['port'] = '5500'
        self.db_info['user'] = 'admin'
        self.db_info['passwd'] = 'pysixdesk'

        # the name of mask file
        mask_file = 'hl10.mask'
        self.madx_input["mask_file"] = mask_file
        # All parameters are case-sensitive
        self.params = StudyParams(os.path.join(self.study_path, mask_file),
                                  fort_path=os.path.join(self.study_path, 'fort.3'),
                                  machine_defaults=machineparams.HLLHC['col'])
        self.params['Runnam'] = name
        amp = [8, 10, 12]
        self.params['amp'] = list(zip(amp, amp[1:]))  # Take pairs
        self.params['kang'] = list(range(1, 1 + 1))  # The angle
        self.params['kmax'] = 5

        # TODO: talk with Xiaohan to figure out how the angle/amplitude stuff
        # works
        def calc_angle(kang, kmax):
            return kang / (kmax + 1)

        self.params.add_calc(['kang', 'kmax'], ['angle'], calc_angle)

        def calc_amp(angle, emit, e0, pmass, amp, pre_id=None):

            def getval(pre_id, reqlist):
                '''Get required values from oneturn sixtrack results'''
                where = 'wu_id=%s' % pre_id
                ids = self.db.select('preprocess_wu', ['task_id'], where)
                if not ids:
                    raise ValueError("Wrong preprocess job id %s!" % pre_id)
                task_id = ids[0][0]
                if task_id is None:
                    raise Exception("Incomplete preprocess job id %s!" % pre_id)
                where = 'task_id=%s' % task_id
                values = self.db.select('oneturn_sixtrack_result', reqlist, where)
                if not values:
                    raise ValueError("Wrong task id %s!" % task_id)
                return values[0]

            values = getval(pre_id, ['betax'])
            beta_x = values[0]
            tt = abs(sin(pi / 2 * angle) / cos(pi / 2 * angle))
            ratio = 0.0 if tt < 1.0E-15 else tt**2
            gamma = e0 / pmass
            factor = sqrt(emit / gamma)
            ax0t = factor * (sqrt(beta_x) + sqrt(beta_x * ratio) * cos(pi / 2 * angle))
            return [a * ax0t for a in amp]

        self.params.add_calc(['angle', 'emit', 'e0', 'pmass', 'amp'],
                             ['ax0s', 'ax1s'],
                             calc_amp)

        self.oneturn_sixtrack_input['temp'] = ['fort.3']
        self.oneturn_sixtrack_output = ['oneturnresult']
        self.sixtrack_input['temp'] = ['fort.3']
        self.sixtrack_input['input'] = copy.deepcopy(self.madx_output)

        # Update the user-define parameters and objects
        self.customize()  # This call is mandatory


