import os
import logging
from . import dbadaptor


class SixDB(object):

    def __init__(self, db_info, settings=None, create=False):
        '''Constructor.
        db_info(dict): contain the information of the database,
                       such as db_name, type, user, password, host and so on
        For sqlite db only db_name is needed which should be an absolute path,
        For MySQL db db_info should contain db_name(just name), user,
        password, host, port and other optional arguments.
        '''
        self._logger = logging.getLogger(__name__)
        self.settings = settings
        info = {}
        info.update(db_info)
        self.info = info
        if self.info['db_type'] == 'mysql':
            self.info['db_name'] = '_'.join([self.info['user'],
                                             self.info['db_name']])
        # if db_type in info's keys pop it's value, if not sql
        db_type = self.info.pop('db_type', 'sql')
        self.db_type = db_type.lower()
        self.open(create=create)

    def open(self, create=False):
        '''Setup the database'''
        if self.db_type not in ['sql', 'mysql']:
            content = "Unkown database type %s!" % self.db_type
            raise ValueError(content)

        if not self.info_check():
            content = "Something wrong with db info %s!" % str(self.info)
            raise ValueError(content)

        if self.db_type == 'sql':
            self.adaptor = dbadaptor.SQLDatabaseAdaptor()
            name = self.info['db_name']
            if not create and not os.path.exists(name):
                content = "The database %s doesn't exist!" % name
                raise Exception(content)

        elif self.db_type == 'mysql':
            self.adaptor = dbadaptor.MySQLDatabaseAdaptor()
            if create:
                self.adaptor.create_db(**self.info)

        self.conn = self.adaptor.new_connection(**self.info)
        if self.settings is not None:
            self.setting(self.settings)

    def setting(self, settings):
        '''Execute the settings of the database'''
        self.adaptor.setting(self.conn, settings)

    def info_check(self):
        '''Check if all the necessary information for database is there.
        And  check if the parameter's type is correct, if not, correct it'''
        if self.db_type == 'sql':
            if 'db_name' in self.info:
                if not isinstance(self.info['db_name'], str):
                    self.info['db_name'] = str(self.info['db_name'])
                return True
            else:
                return False
        elif self.db_type == 'mysql':
            if ('port' in self.info and 'user' in self.info and 'host' in self.info
                    and 'passwd' in self.info and 'db_name' in self.info):
                if not isinstance(self.info['port'], int):
                    self.info['port'] = int(self.info['port'])
                if not isinstance(self.info['user'], str):
                    self.info['user'] = str(self.info['user'])
                if not isinstance(self.info['host'], str):
                    self.info['host'] = str(self.info['host'])
                if not isinstance(self.info['passwd'], str):
                    self.info['passwd'] = str(self.info['passwd'])
                if not isinstance(self.info['db_name'], str):
                    self.info['db_name'] = str(self.info['db_name'])
                return True
            else:
                return False

    def fetch_tables(self):
        '''Get all the table names in the database'''
        r = self.adaptor.fetch_tables(self.conn)
        return r

    def create_table(self, table_name, table_info, key_info={}, recreate=False):
        '''Create a new table or recreate an existing table'''
        self.adaptor.create_table(self.conn, table_name, table_info, key_info,
                                  recreate)

    def create_tables(self, tables, tables_keys={}, recreate=False):
        '''Create multiple tables'''
        for key, value in tables.items():
            key_info = {}
            if key in tables_keys.keys():
                key_info = tables_keys[key]
            self.create_table(key, value, key_info, recreate)

    def drop_table(self, table_name):
        '''Drop a table'''
        self.adaptor.drop_table(self.conn, table_name)

    def insert(self, table_name, values):
        '''Insert a row of values'''
        self.adaptor.insert(self.conn, table_name, values)

    def insertm(self, table_name, values):
        '''Insert multiple rows'''
        self.adaptor.insertm(self.conn, table_name, values)

    def select(self, table_name, columns='*', where=None, orderby=None, **kwargs):
        '''Select values with specified conditions'''
        r = self.adaptor.select(self.conn, table_name, columns, where, orderby,
                                **kwargs)
        return r

    def update(self, table_name, values, where=None):
        '''Update data in a table'''
        self.adaptor.update(self.conn, table_name, values, where)

    def remove(self, table_name, where):
        '''Reomve rows based on specified conditions'''
        self.adaptor.delete(self.conn, table_name, where)

    def close(self):
        '''Disconnect the database'''
        self.conn.commit()
        self.conn.close()

    def __del__(self):
        try:
            self.close()
            content = "Closed database connection."
            self._logger.info(content)
        except Exception:
            pass
