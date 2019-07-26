class SQLiteDict(object):

    def __init__(self):
        self.db_type = {}
        self.db_type['None'] = 'NULL'
        self.db_type['int'] = 'INT'
        self.db_type['float'] = 'float'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'
        self.db_type['tuple'] = 'INT'

    def __getitem__(self, param):
        '''Get the corresponding sqlite data type'''
        if isinstance(param, list):
            param = param[0]
        a = type(param)
        return self.db_type[a.__name__]


class MySQLDict(object):

    def __init__(self):
        self.db_type = {}
        self.db_type['None'] = 'NULL'
        self.db_type['int'] = 'INT'
        self.db_type['float'] = 'DOUBLE'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'
        self.db_type['tuple'] = 'TEXT'

    def __getitem__(self, param):
        '''Get the corresponding mysql data type'''
        if isinstance(param, list):
            param = param[0]
        a = type(param)
        return self.db_type[a.__name__]
