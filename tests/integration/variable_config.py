import os
from pathlib import Path
from importlib.machinery import SourceFileLoader
path = Path(__file__).absolute().parents[2] / 'templates/config.py'
mod = SourceFileLoader('MyStudy', str(path)).load_module()
MyStudy = getattr(mod, 'MyStudy')


class SqlConfig(MyStudy):
    def __init__(self, name='study', location=os.getcwd()):
        super().__init__(name, location)
        self.db_info['db_type'] = 'sql'
        self.customize(fake=False)

    def customize(self, fake=True):
        '''
        Override customize to make the call in super().__init__ useless
        '''
        if not fake:
            super().customize()
        else:
            pass


class MySqlConfig(MyStudy):
    def __init__(self, name='study', location=os.getcwd()):
        super().__init__(name, location)
        self.db_info['db_type'] = 'mysql'
        self.customize(fake=False)

    def customize(self, fake=True):
        '''
        Override customize to make the call in super().__init__ useless
        '''
        if not fake:
            super().customize()
        else:
            pass
