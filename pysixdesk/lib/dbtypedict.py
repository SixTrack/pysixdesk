def bigint_check(val):
    """
    Checks to see if `val` is or contains BIGINTs.

    Args:
        val (int, float, list): value or list of values to check.

    Returns:
        bool: True if `val` contains one or more BIGINT, False if not.
    """
    if not isinstance(val, list) or isinstance(val, str):
        val = [val]
    return any([(type(v) == int and v > 2147483647) for v in val])


class SQLiteDict(object):

    def __init__(self):
        self.db_type = {}
        self.db_type['NoneType'] = 'NULL'
        self.db_type['int'] = 'INT'
        self.db_type['float'] = 'DOUBLE'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'
        self.db_type['tuple'] = 'TEXT'

    def __getitem__(self, param):
        '''Get the corresponding sqlite data type'''
        if isinstance(param, list):
            param_0 = param[0]
        else:
            param_0 = param
        a = type(param_0)
        sql_type = self.db_type[a.__name__]
        if sql_type == 'INT' and bigint_check(param):
            return 'BIGINT'
        else:
            return sql_type


class MySQLDict(object):

    def __init__(self):
        self.db_type = {}
        self.db_type['NoneType'] = 'NULL'
        self.db_type['int'] = 'INT'
        self.db_type['float'] = 'DOUBLE'
        self.db_type['str'] = 'TEXT'
        self.db_type['bytes'] = 'BLOB'
        self.db_type['tuple'] = 'TEXT'

    def __getitem__(self, param):
        '''Get the corresponding mysql data type'''
        if isinstance(param, list):
            param_0 = param[0]
        else:
            param_0 = param
        a = type(param_0)
        sql_type = self.db_type[a.__name__]
        if sql_type == 'INT' and bigint_check(param):
            return 'BIGINT'
        else:
            return sql_type
