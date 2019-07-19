import sys
import utils
import sqlite3
import pymysql
import collections
import traceback
from abc import ABC, abstractmethod


class DatabaseAdaptor(ABC):

    def __init__(self, mes_level, log_file):
        self.mes_level = mes_level
        self.log_file = log_file

    @abstractmethod
    def new_connection(self, name):
        pass

    @abstractmethod
    def setting(self, settings):
        pass

    def create_table(self, conn, name, columns, keys, recreate):
        '''Create a new table'''
        c = conn.cursor()
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
        conn.commit()

    def drop_table(self, conn, table_name):
        '''Drop an exist table'''
        c = conn.cursor()
        sql = 'DROP TABLE IF EXISTS %s' % table_name
        c.execute(sql)
        conn.commit()

    def insert(self, conn, table_name, values, ph):
        '''Insert a row of values
        @conn A connection of database
        @table_name(str) The table name
        @values(dict) The values required to insert into database
        @ph The placeholder for the selected database, e.g. ?, %s
        '''
        if len(values) == 0:
            return
        c = conn.cursor()
        sql = 'INSERT INTO %s (%s) VALUES (%s)'
        keys = list(values.keys())
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        cols = ','.join(keys)
        ques = ','.join((ph,) * len(keys))
        sql_cmd = sql % (table_name, cols, ques)
        c.execute(sql_cmd, vals)
        conn.commit()

    def insertm(self, conn, table_name, values, ph):
        '''Insert multiple rows once
        @conn A connection of database
        @table_name(str) The table name
        @values(dict) The values required to insert into database
        @ph The placeholder for the selected database, e.g. ?, %s
        '''
        if len(values) == 0:
            return
        c = conn.cursor()
        sql = 'INSERT INTO %s (%s) VALUES (%s)'
        keys = list(values.keys())
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        cols = ','.join(keys)
        ques = ','.join((ph,) * len(keys))
        sql_cmd = sql % (table_name, cols, ques)
        vals = list(zip(*vals))
        c.executemany(sql_cmd, vals)
        conn.commit()

    def select(self, conn, table_name, cols='*', where=None, orderby=None, **kwargs):
        '''Select values with conditions
        @conn A connection of database
        @table_name(str) The table name
        @cols(list) The column names
        @where(str) Selection condition
        @orderby(list) Order condition
        @**kwargs Some other conditions
        '''
        if len(cols) == 0:
            return []
        c = conn.cursor()
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

    def update(self, conn, table_name, values, where, ph):
        '''Update data in a table
        @conn A connection of database
        @table_name(str) The table name
        @values(dict) The column names with new values
        @where(str) Selection condition
        @ph The placeholder for the selected database, e.g. ?, %s
        '''

        if len(values) == 0:
            return
        c = conn.cursor()
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
        conn.commit()

    def delete(self, conn, table_name, where):
        '''Update data in a table
        @conn A connection of database
        @table_name(str) The table name
        @where(str) Selection condition which is mandatory here!
        '''
        c = conn.cursor()
        sql = 'DELETE FROM %s WHERE %s' % (table_name, where)
        c.execute(sql)
        conn.commit()


class SQLDatabaseAdaptor(DatabaseAdaptor):

    def __init__(self, mes_level=1, log_file=None):
        DatabaseAdaptor.__init__(self, mes_level, log_file)
        self.mes_level = mes_level
        self.log_file = log_file

    def new_connection(self, db_name, **kwargs):
        '''Create a new connection'''
        if '.db' not in db_name:
            db_name = db_name + '.db'
        conn = sqlite3.connect(db_name)
        return conn

    def setting(self, conn, settings):
        '''Execute the settings of the database via pragma command'''
        c = conn.cursor()
        for key, value in settings.items():
            sql = 'PRAGMA %s=%s' % (key, str(value))
            c.execute(sql)
        conn.commit()

    def create_table(self, conn, name, columns, keys, recreate):
        '''Create a new table'''
        if 'autoincrement' in keys.keys():
            auto_keys = keys.pop('autoincrement')
            if 'primary' in keys.keys():
                prim_keys = keys['primary']
                for ky in auto_keys:
                    if ky in prim_keys and columns[ky] != 'INTEGER':
                        columns[ky] = 'INTEGER'
        super(SQLDatabaseAdaptor, self).create_table(conn, name, columns, keys,
                                                     recreate)

    def fetch_tables(self, conn):
        '''Fetch all the table names in the database'''
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return list(c)

    def insert(self, conn, table_name, values):
        '''Insert a row of values'''
        super(SQLDatabaseAdaptor, self).insert(conn, table_name, values, '?')

    def insertm(self, conn, table_name, values):
        '''Insert multi rows of values'''
        super(SQLDatabaseAdaptor, self).insertm(conn, table_name, values, '?')

    def update(self, conn, table_name, values, where):
        '''update values'''
        super(SQLDatabaseAdaptor, self).update(
            conn, table_name, values, where, '?')


class MySQLDatabaseAdaptor(DatabaseAdaptor):

    def __init__(self, mes_level=1, log_file=None):
        DatabaseAdaptor.__init__(self, mes_level, log_file)
        self.mes_level = mes_level
        self.log_file = log_file

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
        '''Connect to an exist database'''
        try:
            conn = pymysql.connect(host, user, passwd, db_name, **kwargs)
            return conn
        except:
            content = traceback.print_exc()
            utils.message('Error', content, self.mes_level, self.log_file)
            content = "Failed to connect to databae %s!" % db_name
            utils.message('Error', content, self.mes_level, self.log_file)
            return

    def setting(self, conn, settings):
        pass

    def create_table(self, conn, name, columns, keys, recreate):
        '''Create a new table'''
        super(MySQLDatabaseAdaptor, self).create_table(conn, name, columns,
                                                       keys, recreate)

    def fetch_tables(self, conn):
        '''Fetch all the table names in the database'''
        c = conn.cursor()
        # c.execute("show databases")
        c.execute("show tables")
        a = list(c)
        return a

    def insert(self, conn, table_name, values):
        '''Insert a row of values'''
        super(MySQLDatabaseAdaptor, self).insert(
            conn, table_name, values, '%s')

    def insertm(self, conn, table_name, values):
        '''Insert multi rows of values'''
        super(MySQLDatabaseAdaptor, self).insertm(
            conn, table_name, values, '%s')

    def update(self, conn, table_name, values, where):
        '''update values'''
        super(MySQLDatabaseAdaptor, self).update(
            conn, table_name, values, where, '%s')
