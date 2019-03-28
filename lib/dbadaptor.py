import os
import sys
import sqlite3
from abc import ABC, abstractmethod

class DatabaseAdaptor(ABC):

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def new_connection(self, name):
        pass

    @abstractmethod
    def create_table(self, conn, tables):
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

    def create_table(self, conn, name, columns):
        '''Create a new table'''
        c = conn.cursor()
        sql = 'CREATE TABLE IF NOT EXISTS ' + name + '('
        col = [' '.join(map(str, i)) for i in columns.items()]
        col = [j.replace('.', '_') for j in col]
        cols = ','.join(map(str, col))
        sql = sql + cols + ')'
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

    def select(self, conn, name, cols='*', where=None, orderby=None, **args):
        '''Select values with conditions
        @conn A connection of database
        @table_name(str) The table name
        @cols(list) The column names
        @where(str) Selection condition
        @orderby(list) Order condition
        @**args Some other conditions
        '''
        c = conn.cursor()
        cols = ','.join(cols)
        sql = 'SELECT %s FROM %s'%(cols, table_name)
        if where is not None:
            sql += ' WHERE %s'%where
        if orderby is not None:
            sql += ' ORDER BY %s'(','.join(orderby))
        c.execute(sql)
        data = list(c)
        return data

    def delete(self, conn, table_name, **conditions):
        pass

class MySQLDatabaseAdaptor(DatabaseAdaptor):

    def __init__(self, name):
        DatabaseAdaptor.__init__(self, name)

    def new_connection(self, name):
        pass

    def create_table(self, conn, tables):
        pass

    def fetch_tables(self, command):
        pass

    def insert(self):
        pass

    def select(self):
        pass

    def delete(self):
        pass

if __name__ == '__main__':
    a=SQLDatabaseAdaptor('test')
    conn = a.new_connection('test')
    b={'name':'text','age':'INT'}
    a.create_table(conn, 'tab', b)
    print(a.fetch_tables(conn))
    #b=MySQLDatabaseAdaptor('test')
