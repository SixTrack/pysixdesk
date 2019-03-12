import os
import sys
import dbadaptor

class SixDB(object):

    def __init__(self, name):
        self.name = name

    def setup(sefl, tables, dbtype = 'sql', **args):
        if dbtype == 'sql':
            adaptor = dbadaptor.SQLDatabaseAdaptor()
        elif dbtype == 'mysql':
            adaptor = dbadaptor.MySQLDatabaseAdaptor()
        else:
            print("Unkonw database type!")
            sys.exit(0)

        adaptor.new_database(tables)
