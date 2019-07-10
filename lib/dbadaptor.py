import os
import sys
import utils
import sqlite3
import pymysql
import collections
import traceback
from abc import ABC, abstractmethod


class DatabaseAdaptor(ABC):

    def __init__(self, db_info, settings=None, create=False, mes_level=1, log_file=None):
        self.settings = settings
        self.create = create
        self.mes_level = mes_level
        self.log_file = log_file
        self.db_info = db_info
        self.conn = None
        self._check()
        self._setup()

    @abstractmethod
    def _check(self):
        pass

    def _setup(self):
        self.conn = self.new_connection(**self.db_info)
        if self.settings is not None:
            self.setting(self.settings)

    @abstractmethod
    def new_connection(self, name):
        pass

    @abstractmethod
    def setting(self, settings):
        pass

    def create_tables(self, tables, tables_keys={}, recreate=False):
        '''Create multiple tables'''
        for key, value in tables.items():
            key_info = {}
            if key in tables_keys.keys():
                key_info = tables_keys[key]
            self.create_table(key, value, key_info, recreate)

    def create_table(self, name, columns, keys, recreate):
        '''Create a new table'''
        c = self.conn.cursor()
        if recreate:
            c.execute("DROP TABLE IF EXISTS %s" % name)
        sql = 'CREATE TABLE IF NOT EXISTS %s (%s)'
        if 'autoincrement' in keys.keys():
            auto_keys = keys['autoincrement']
            for ky in auto_keys:
                columns[ky] = columns[ky] + ' AUTO_INCREMENT'
        col = [' '.join(map(str, i)) for i in columns.items()]
        col = [j.replace('.', '_') for j in col]
        cols = ','.join(map(str, col))
        fill = cols
        if 'primary' in keys.keys():
            prim = keys['primary']
            if prim:
                prim_key = ','.join(map(str, prim))
                prim_sql = ', PRIMARY KEY(%s)' % prim_key
                fill += prim_sql
        if 'foreign' in keys.keys():
            fore = keys['foreign']
            if fore:
                for k in fore.keys():
                    fore_k = ','.join(fore[k][0])
                    ref_k = ','.join(fore[k][1])
                    fore_sql = ', FOREIGN KEY(%s) REFERENCES %s(%s) ON UPDATE\
                                CASCADE ON DELETE CASCADE' % (fore_k, k, ref_k)
                    fill += fore_sql
        sql_cmd = sql % (name, fill)
        c.execute(sql_cmd)
        self.conn.commit()

    def drop_table(self, table_name):
        '''Drop an exist table'''
        c = self.conn.cursor()
        sql = 'DROP TABLE IF EXISTS %s' % table_name
        c.execute(sql)
        self.conn.commit()

    def insert(self, table_name, values, ph):
        '''Insert a row of values
        @table_name(str) The table name
        @values(dict) The values required to insert into database
        @ph The placeholder for the selected database, e.g. ?, %s
        '''
        if len(values) == 0:
            return
        c = self.conn.cursor()
        sql = 'INSERT INTO %s (%s) VALUES (%s)'
        keys = list(values.keys())
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        cols = ','.join(keys)
        ques = ','.join((ph,) * len(keys))
        sql_cmd = sql % (table_name, cols, ques)
        c.execute(sql_cmd, vals)
        self.conn.commit()

    def insertm(self, table_name, values, ph):
        '''Insert multiple rows once
        @table_name(str) The table name
        @values(dict) The values required to insert into database
        @ph The placeholder for the selected database, e.g. ?, %s
        '''
        if len(values) == 0:
            return
        c = self.conn.cursor()
        sql = 'INSERT INTO %s (%s) VALUES (%s)'
        keys = list(values.keys())
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        cols = ','.join(keys)
        ques = ','.join((ph,) * len(keys))
        sql_cmd = sql % (table_name, cols, ques)
        vals = list(zip(*vals))
        c.executemany(sql_cmd, vals)
        self.conn.commit()

    def select(self, table_name, cols='*', where=None, orderby=None, **kwargs):
        '''Select values with conditions
        @table_name(str) The table name
        @cols(list) The column names
        @where(str) Selection condition
        @orderby(list) Order condition
        @**kwargs Some other conditions
        '''
        if len(cols) == 0:
            return []
        c = self.conn.cursor()
        if (isinstance(cols, collections.Iterable) and not isinstance(cols, str)):
            cols = [i.replace('.', '_') for i in cols]
            cols = ','.join(cols)
        sql = 'SELECT %s FROM %s' % (cols, table_name)
        if 'DISTINCT' in kwargs.keys() and kwargs['DISTINCT']:
            sql = 'SELECT DISTINCT %s FROM %s' % (cols, table_name)
        if where is not None:
            sql += ' WHERE %s' % where
        if orderby is not None:
            sql += ' ORDER BY %s'(','.join(orderby))
        c.execute(sql)
        data = list(c)
        return data

    def update(self, table_name, values, ph, where=None):
        '''Update data in a table
        @table_name(str) The table name
        @values(dict) The column names with new values
        @where(str) Selection condition
        @ph The placeholder for the selected database, e.g. ?, %s
        '''

        if len(values) == 0:
            return
        c = self.conn.cursor()
        sql = 'UPDATE %s SET %s '
        keys = values.keys()
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        ques = (ph,) * len(keys)
        sets = ['='.join(it) for it in zip(keys, ques)]
        sets = ','.join(sets)
        if where is not None:
            sql = sql + 'WHERE ' + where
        sql_cmd = sql % (table_name, sets)
        c.execute(sql_cmd, vals)
        self.conn.commit()

    def remove(self, table_name, where):
        '''Remove rows based on specified conditions
        @table_name(str) The table name
        @where(str) Selection condition which is mandatory here!
        '''
        c = self.conn.cursor()
        sql = 'DELETE FROM %s WHERE %s' % (table_name, where)
        c.execute(sql)
        self.conn.commit()

    def close(self):
        '''Disconnect the database, if a connection is active'''
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()

    def __del__(self):
        '''Disconnect before deletion'''
        self.close()


class SQLDatabaseAdaptor(DatabaseAdaptor):

    def _check(self):
        if self._info_check():
            name = self.db_info['db_name']
            if not self.create and not os.path.exists(name):
                content = "The database %s doesn't exist!" % name
                utils.message('Error', content, self.mes_level, self.log_file)
                sys.exit(1)
        else:
            content = "Something wrong with db info %s!" % str(self.db_info)
            utils.message('Error', content, self.mes_level, self.log_file)
            sys.exit(1)  # This used to not exit

    def _info_check(self):
        '''
        Check if all the necessary information for database is there.
        And check if the parameter's type is correct, if not, correct it
        '''
        if 'db_name' in self.db_info:
            if not isinstance(self.db_info['db_name'], str):
                self.db_info['db_name'] = str(self.db_info['db_name'])
            return True
        else:
            return False

    def new_connection(self, db_name, **kwargs):
        '''Create a new connection'''
        if '.db' not in db_name:
            db_name = db_name + '.db'
        conn = sqlite3.connect(db_name)
        return conn

    def setting(self, settings):
        '''Execute the settings of the database via pragma command'''
        c = self.conn.cursor()
        for key, value in settings.items():
            sql = 'PRAGMA %s=%s' % (key, str(value))
            c.execute(sql)
        self.conn.commit()

    def create_table(self, name, columns, keys, recreate):
        '''Create a new table'''
        if 'autoincrement' in keys.keys():
            auto_keys = keys.pop('autoincrement')
            if 'primary' in keys.keys():
                prim_keys = keys['primary']
                for ky in auto_keys:
                    if ky in prim_keys and columns[ky] != 'INTEGER':
                        columns[ky] = 'INTEGER'
        super(SQLDatabaseAdaptor, self).create_table(name, columns, keys, recreate)

    def fetch_tables(self):
        '''Fetch all the table names in the database'''
        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return list(c)

    def insert(self, table_name, values):
        '''Insert a row of values'''
        super(SQLDatabaseAdaptor, self).insert(table_name, values, '?')

    def insertm(self, table_name, values):
        '''Insert multi rows of values'''
        super(SQLDatabaseAdaptor, self).insertm(table_name, values, '?')

    def update(self, table_name, values, where=None):
        '''update values'''
        super(SQLDatabaseAdaptor, self).update(table_name, values, '?', where=where)


class MySQLDatabaseAdaptor(DatabaseAdaptor):

    def _check(self):
        if self._info_check():
            if self.create:
                self.create_db(**self.db_info)
        else:
            content = "Something wrong with db info %s!" % str(self.db_info)
            utils.message('Error', content, self.mes_level, self.log_file)
            sys.exit(1)

    def _info_check(self):
        '''
        Check if all the necessary information for database is there.
        And check if the parameter's type is correct, if not, correct it
        '''
        keys = ['port', 'user', 'host', 'passwd', 'db_name']
        if all([k in self.db_info for k in keys]):
            if not isinstance(self.db_info['port'], int):
                self.db_info['port'] = int(self.db_info['port'])
            if not isinstance(self.db_info['user'], str):
                self.db_info['user'] = str(self.db_info['user'])
            if not isinstance(self.db_info['host'], str):
                self.db_info['host'] = str(self.db_info['host'])
            if not isinstance(self.db_info['passwd'], str):
                self.db_info['passwd'] = str(self.db_info['passwd'])
            if not isinstance(self.db_info['db_name'], str):
                self.db_info['db_name'] = str(self.db_info['db_name'])
            return True
        else:
            return False

    def create_db(self, host, user, passwd, db_name, **kwargs):
        '''Create a new database'''
        try:
            conn = pymysql.connect(host, user, passwd, **kwargs)
            c = conn.cursor()
            sql = "SELECT schema_name FROM information_schema.schemata\
                    WHERE schema_name='%s'" % db_name
            c.execute(sql)
        except:
            content = traceback.print_exc()
            utils.message('Error', content, self.mes_level, self.log_file)
            sys.exit(1)

        if not list(c):
            try:
                sql = "CREATE DATABASE %s" % db_name
                c.execute(sql)
                conn.commit()
            except:
                content = traceback.print_exc()
                utils.message('Error', content, self.mes_level, self.log_file)
                content = "Failed to create new db %s!" % db_name
                utils.message('Error', content, self.mes_level, self.log_file)
                conn.rollback()
            finally:
                conn.close()
        else:
            content = "The db %s already exist!" % db_name
            utils.message('Warning', content, self.mes_level, self.log_file)

    def new_connection(self, host, user, passwd, db_name, **kwargs):
        '''Connect to an existing database'''
        conn = pymysql.connect(host, user, passwd, db_name, **kwargs)
        return conn

    def setting(self, settings):
        pass

    def create_table(self, name, columns, keys, recreate):
        '''Create a new table'''
        super(MySQLDatabaseAdaptor, self).create_table(name, columns, keys, recreate)

    def fetch_tables(self):
        '''Fetch all the table names in the database'''
        c = self.conn.cursor()
        # c.execute("show databases")
        c.execute("show tables")
        a = list(c)
        return a

    def insert(self, table_name, values):
        '''Insert a row of values'''
        super(MySQLDatabaseAdaptor, self).insert(table_name, values, '%s')

    def insertm(self, table_name, values):
        '''Insert multi rows of values'''
        super(MySQLDatabaseAdaptor, self).insertm(table_name, values, '%s')

    def update(self, table_name, values, where=None):
        '''update values'''
        super(MySQLDatabaseAdaptor, self).update(table_name, values, '%s', where=where)
