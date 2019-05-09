import os
import sys
import time
import shutil
import gzip
import dbadaptor

class SixDB(object):

    def __init__(self, name, settings=None, create=False, dbtype='sql'):
        self.name = name #absolute path of the database in a study folder
        self.dbtype = dbtype
        self.settings = settings
        if not create and not os.path.exists(name):
            print("The database %s doesn't exist!"%name)
            sys.exit(1)
        else:
            self._setup()

    def _setup(self):
        '''Setup the database with the given tables'''
        if self.dbtype.lower() == 'sql':
            self.adaptor = dbadaptor.SQLDatabaseAdaptor()
        elif self.dbtype.lower() == 'mysql':
            self.adaptor = dbadaptor.MySQLDatabaseAdaptor()
        else:
            print("Unkonw database type!")
            sys.exit(0)

        self.conn = self.adaptor.new_connection(self.name)
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
