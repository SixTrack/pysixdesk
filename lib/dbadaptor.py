import os
import sys
import sqlite3
import collections
from abc import ABC, abstractmethod

class DatabaseAdaptor(ABC):

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def new_connection(self, name):
        pass

    @abstractmethod
    def setting(self, settings):
        pass

    @abstractmethod
    def create_table(self, conn, table):
        pass

    @abstractmethod
    def drop_table(self, conn, table):
        pass

    @abstractmethod
    def fetch_tables(self, connection):
        pass

    @abstractmethod
    def insert(self):
        pass

    @abstractmethod
    def select(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def delete(self):
        pass

class SQLDatabaseAdaptor(DatabaseAdaptor):

    def __init__(self, name = 'no name'):
        DatabaseAdaptor.__init__(self, name)

    def new_connection(self, name):
        '''Create a new connection'''
        if not '.db' in name:
            name = name + '.db'
        conn = sqlite3.connect(name)
        return conn

    def setting(self, conn, settings):
        '''Execute the settings of the database via pragma command'''
        c = conn.cursor()
        for key, value in settings.items():
            sql = 'PRAGMA %s=%s'%(key, str(value))
            c.execute(sql)
        conn.commit()

    def create_table(self, conn, name, columns, keys, recreate):
        '''Create a new table'''
        c = conn.cursor()
        if recreate:
            c.execute("DROP TABLE IF EXISTS %s"%name)
        sql = 'CREATE TABLE IF NOT EXISTS %s (%s)'
        col = [' '.join(map(str, i)) for i in columns.items()]
        col = [j.replace('.', '_') for j in col]
        cols = ','.join(map(str, col))
        fill = cols
        if 'primary' in keys.keys():
            prim = keys['primary']
            if prim:
                prim_key = ','.join(map(str,prim))
                prim_sql = ', PRIMARY KEY(%s)'%prim_key
                fill += prim_sql
        if 'foreign' in keys.keys():
            fore = keys['foreign']
            if fore:
                for k in fore.keys():
                    fore_k = ','.join(fore[k][0])
                    ref_k = ','.join(fore[k][1])
                    fore_sql = ', FOREIGN KEY(%s) REFERENCES %s(%s) ON UPDATE\
                                CASCADE ON DELETE CASCADE'%(fore_k, k, ref_k)
                    fill += fore_sql
        sql_cmd = sql%(name, fill)
        c.execute(sql_cmd)
        conn.commit()

    def drop_table(self, conn, table_name):
        '''Drop an exist table'''
        c = conn.cursor()
        sql = 'DROP TABLE IF EXISTS %s'%table_name
        c.execute(sql)
        conn.commit()

    def fetch_tables(self, conn):
        '''Fetch all the table names in the database'''
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return c.fetchall()

    def insert(self, conn, table_name, values):
        '''Insert a row of values
        @conn A connection of database
        @table_name(str) The table name
        @values(dict) The values required to insert into database
        '''
        c = conn.cursor()
        sql = 'INSERT INTO %s (%s) VALUES (%s)'
        keys = list(values.keys())
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        cols = ','.join(keys)
        ques = ','.join(('?',)*len(keys))
        sql_cmd = sql%(table_name, cols, ques)
        c.execute(sql_cmd, vals)
        conn.commit()

    def insertm(self, conn, table_name, values):
        '''Insert multiple rows once
        @conn A connection of database
        @table_name(str) The table name
        @values(dict) The values required to insert into database
        '''
        c = conn.cursor()
        sql = 'INSERT INTO %s (%s) VALUES (%s)'
        keys = list(values.keys())
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        cols = ','.join(keys)
        ques = ','.join(('?',)*len(keys))
        sql_cmd = sql%(table_name, cols, ques)
        vals = list(zip(*vals))
        c.executemany(sql_cmd, vals)
        conn.commit()

    def select(self, conn, table_name, cols='*', where=None, orderby=None, **args):
        '''Select values with conditions
        @conn A connection of database
        @table_name(str) The table name
        @cols(list) The column names
        @where(str) Selection condition
        @orderby(list) Order condition
        @**args Some other conditions
        '''
        c = conn.cursor()
        if isinstance(cols, collections.Iterable):
            cols = [i.replace('.', '_') for i in cols]
            cols = ','.join(cols)
        sql = 'SELECT %s FROM %s'%(cols, table_name)
        if where is not None:
            sql += ' WHERE %s'%where
        if orderby is not None:
            sql += ' ORDER BY %s'(','.join(orderby))
        c.execute(sql)
        data = list(c)
        return data

    def update(self, conn, table_name, values, where=None):
        '''Update data in a table
        @conn A connection of database
        @table_name(str) The table name
        @values(dict) The column names with new values
        @where(str) Selection condition
        '''
        c = conn.cursor()
        sql = 'UPDATE %s SET %s '
        keys = values.keys()
        vals = [values[key] for key in keys]
        keys = [i.replace('.', '_') for i in keys]
        ques = ('?',)*len(keys)
        sets = ['='.join(it) for it in zip(keys, ques)]
        sets = ','.join(sets)
        if where is not None:
            sql = sql + 'WHERE ' + where
        sql_cmd = sql%(table_name, sets)
        c.execute(sql_cmd, vals)
        conn.commit()

    def delete(self, conn, table_name, where):
        '''Update data in a table
        @conn A connection of database
        @table_name(str) The table name
        @where(str) Selection condition which is mandatory here!
        '''
        c = conn.cursor()
        sql = 'DELETE FROM %s WHERE %s'%(table_name, where)
        c.execute(sql)
        conn.commit()

class MySQLDatabaseAdaptor(DatabaseAdaptor):

    def __init__(self, name):
        DatabaseAdaptor.__init__(self, name)

    def new_connection(self, name):
        pass

    def setting(self, conn, settings):
        pass

    def create_table(self, conn, tables):
        pass

    def drop_table(self, conn, table_name):
        pass

    def fetch_tables(self, command):
        pass

    def insert(self):
        pass

    def select(self):
        pass

    def delete(self):
        pass
