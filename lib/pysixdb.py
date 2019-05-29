import os
import sys
import time
import shutil
import gzip
import dbadaptor

class SixDB(object):

    def __init__(self, db_info, settings=None, create=False):
        '''Constructor.
        db_info(dict): contain the information of the database,
                       such as db_name, type, user, password, host and so on
        For sqlite db only db_name is needed which should be an absolute path,
        For MySQL db db_info should contain db_name(just name), user,
        password, host, port and other optional arguments.
        '''
        self.settings = settings
        self.create = create
        self._setup(db_info)

    def _setup(self, db_info):
        '''Setup the database'''
        if 'db_type' not in db_info.keys():
            dbtype = 'sql'
        else:
            dbtype = db_info['db_type']
        if dbtype.lower() == 'sql':
            self.adaptor = dbadaptor.SQLDatabaseAdaptor()
            name = db_info['db_name']
            if not self.create and not os.path.exists(name):
                print("The database %s doesn't exist!"%name)
                sys.exit(1)
        elif dbtype.lower() == 'mysql':
            self.adaptor = dbadaptor.MySQLDatabaseAdaptor()
            if self.create:
                self.adaptor.create_db(**db_info)
        else:
            print("Unkonw database type!")
            sys.exit(0)

        self.conn = self.adaptor.new_connection(**db_info)
        if self.settings is not None:
            self.setting(self.settings)

    def setting(self, settings):
        '''Execute the settings of the database'''
        self.adaptor.setting(self.conn, settings)

    def fetch_tables(self):
        '''Get all the table names in the database'''
        r = self.adaptor.fetch_tables(self.conn)
        return r

    def create_table(self, table_name, table_info, key_info={}, recreate=False):
        '''Create a new table or recreate an existing table'''
        self.adaptor.create_table(self.conn, table_name, table_info, key_info,\
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

    def select(self, table_name, columns='*', where=None, orderby=None, **args):
        '''Select values with specified conditions'''
        r = self.adaptor.select(self.conn, table_name, columns, where, orderby,
                **args)
        return r

    def update(self, table_name, values, where=None):
        '''Update data in a table'''
        self.adaptor.update(self.conn, table_name, values, where)

    def remove(self, table_name, where=None):
        '''Reomve rows based on specified conditions'''
        self.adaptor.delete(self.conn, table_name, where)

    def close(self):
        '''Disconnect the database'''
        self.conn.commit()
        self.conn.close()
