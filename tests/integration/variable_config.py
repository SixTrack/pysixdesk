from pathlib import Path
from importlib.machinery import SourceFileLoader
path = Path(__file__).absolute().parents[2] / 'templates/config.py'
mod = SourceFileLoader('MyStudy', str(path)).load_module()
MyStudy = getattr(mod, 'MyStudy')


# config "fragments":
class MyStudyCustomizable(MyStudy):

    def customize(self, fake=True):
        '''Override customize to make the call in MyStudy.__init__ useless
        '''
        if not fake:
            super().customize()

    def apply_settings(self):
        '''Placeholder for subclasses.
        '''
        pass


class SqlSettings(MyStudyCustomizable):
    def apply_settings(self):
        super().apply_settings()
        print("Using SqlLite")
        self.db_info['db_type'] = 'sql'


class MySqlSettings(MyStudyCustomizable):
    def apply_settings(self):
        super().apply_settings()
        print('Using MySql')
        self.db_info['db_type'] = 'mysql'


class CollSettings(MyStudyCustomizable):
    def apply_settings(self):
        super().apply_settings()
        print("Using Collimation")
        # The parameters for collimation job
        self.oneturn = True
        self.collimation = True
        self.madx_input["mask_file"] = 'collimation.mask'

        self.madx_output = {
            'fc.2': 'fort.2',
            'fc.3': 'fort.3.mad',
            'fc.3.aux': 'fort.3.aux',
            'fc.8': 'fort.8'}
        self.collimation_input = {'aperture': 'allapert.b1',
                                  'survey': 'SurveyWithCrossing_XP_lowb.dat'}
        self.oneturn_sixtrack_input['input'] = dict(self.madx_output)
        self.preprocess_output = dict(self.madx_output)
        self.sixtrack_input['temp'] = 'fort.3'
        self.sixtrack_input['input'] = self.preprocess_output
        self.sixtrack_input['additional_input'] = ['CollDB.data']
        self.sixtrack_output = ['aperture_losses.dat',
                                'coll_summary.dat',
                                'Coll_Scatter.dat']
        self.sixtrack_params = dict(self.oneturn_sixtrack_params)
        self.sixtrack_params['COLL'] = '/'
        self.oneturn_sixtrack_params['COLL'] = ''
        self.sixtrack_params['turnss'] = 100
        self.sixtrack_params['nss'] = 5000
        self.sixtrack_params['ax0s'] = 0
        self.sixtrack_params['ax1s'] = 17
        self.sixtrack_params['e0'] = 6500000
        self.oneturn_sixtrack_params['e0'] = 6500000
        self.sixtrack_params['POST'] = '/'
        self.sixtrack_params['POS1'] = '/'
        self.oneturn_sixtrack_params['POST'] = 'POST'
        self.oneturn_sixtrack_params['POS1'] = ''
        self.sixtrack_params['dp2'] = 0.00
        self.sixtrack_params['ition'] = 1
        self.sixtrack_params['ibtype'] = 1
        self.oneturn_sixtrack_params['ibtype'] = 1
        # self.sixtrack_params['length'] = 26658.864
        # eigen-emittances to be chosen to determine the coupling angle
        self.sixtrack_params['EI'] = 3.5
        self.oneturn_sixtrack_params['EI'] = 3.5
        # logical switch to calculate 4D(ilin=1) or DA approach 6D (ilin=2)
        self.sixtrack_params['ilin'] = 1
        self.oneturn_sixtrack_params['ilin'] = 1

    # Disable pre_calc
    def pre_calc(self, *args, **kwargs):
        return True


class CheckpointSettings(MyStudyCustomizable):
    def apply_settings(self):
        super().apply_settings()
        print('Using Checkpoint')
        # For CR
        self.checkpoint_restart = True
        self.first_turn = 101
        self.last_turn = 200
        self.paths['sixtrack_exe'] = str(Path(__file__).parents[1] / 'sixtrack_cr')


# complete configs:
class MySqlConfig(MySqlSettings):
    def __init__(self, name, location):
        super().__init__(name, location)
        self.apply_settings()
        self.customize(fake=False)


class MySqlCollConfig(CollSettings, MySqlSettings):
    def __init__(self, name, location):
        super().__init__(name, location)
        self.apply_settings()
        self.customize(fake=False)


class MySqlCheckpointConfig(CheckpointSettings, MySqlSettings):
    def __init__(self, name, location):
        super().__init__(name, location)
        self.apply_settings()
        self.customize(fake=False)


class SqliteConfig(SqlSettings):
    def __init__(self, name, location):
        super().__init__(name, location)
        self.apply_settings()
        self.customize(fake=False)


class SqliteCollConfig(CollSettings, SqlSettings):
    def __init__(self, name, location):
        super().__init__(name, location)
        self.apply_settings()
        self.customize(fake=False)


class SqliteCheckpointConfig(CheckpointSettings, SqlSettings):
    def __init__(self, name, location):
        super().__init__(name, location)
        self.apply_settings()
        self.customize(fake=False)
