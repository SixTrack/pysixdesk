import os
import sys
import sqlite3
from abc import ABC, abstractmethod
class DatabaseAdaptor(ABC):

    def __init__(self, name = 'no name'):
        self.name = name

    @abstractmethod
    def new_connection(self, name):
        pass

    @abstractmethod
    def fetch_tables(self, command):
        pass

    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_values(self, table, name):
        pass

class SQLDatabaseAdaptor(DatabaseAdaptor):

    def __init__(self, name):
        DatabaseAdaptor.__init__(self, name)

    def new_connection(self, name):
        if not '.db' in name:
            name = name + '.db'
        conn = sqlite3.connect(name)
        return conn

    def fetch_tables(self, conn):
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        print(c.fetchone())
        print(c.fetchone())
        print(c.fetchall())
        return c.fetchall()

    def get_values(self):
        pass

    def get_columns(self):
        pass

class MySQLDatabaseAdaptor(DatabaseAdaptor):

    def __init__(self, name):
        DatabaseAdaptor.__init__(self, name)

    def new_connection(self, name):
        pass

    def fetch_tables(self, command):
        pass

    def get_values(self):
        pass

    def get_columns(self):
        pass

if __name__ == '__main__':
    a=SQLDatabaseAdaptor('test')
    b=a.new_connection('hl10.db')
    a.fetch_tables(b)
