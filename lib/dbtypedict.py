import os

class SQLiteDict(object):

    def __init__(self):
        self.db_type={}
        self.db_type['None'] = 'NULL'
        self.db_type['int'] = 'INTEGER'
        self.db_type['float'] = 'REAL'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'

    def dbtype(self, param):
        '''Get the corresponding sqlite data type'''
        if isinstance(param, list):
            param = param[0]
        a = type(param)
        return self.db_type[a.__name__]

class MySQLDict(object):

    def __init__(self):
        self.db_type={}
        self.db_type['None'] = 'NULL'
        self.db_type['int'] = 'INTEGER'
        self.db_type['float'] = 'REAL'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'

    def dbtype(self, param):
        '''Get the corresponding mysql data type'''
        if isinstance(param, list):
            param = param[0]
        a = type(param)
        return self.db_type[a.__name__]
