import os
import sys
import time
import shutil
import gzip
import dbadaptor

class SixDB(object):

    def __init__(self, name, create=False, dbtype='sql'):
        self.name = name #absolute path of the database in a study folder
        self.dbtype = dbtype
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

    def transfer_madx_oneturn_res(self, result_path, tables):
        '''Parse the results of madx and oneturn sixtrack jobs and store in
        database'''
        if not os.path.exists(self.name):
            print("You should create the database and create tables at first!")
        else:
            #result_path = study.paths["madx_output"]
            result_path = os.path.join(result_path, 'mad6t_output')
            #madx_out = list(study.tables['mad6t_run'].keys())
            madx_out = list(tables['mad6t_run'].keys())
            if os.listdir(result_path):
                for item in os.listdir(result_path):
                    item = os.path.join(result_path, item)
                    output = self.assemble_result(item, madx_out)
                    self.insert('mad6t_run', output)
            else:
                print("There is no result in study %s!"%study_path)

    def assemble_result(self, path, out_names):
        '''Process a result of madx and oneturn sixtrack jobs for database'''
        output = {}
        if os.path.isdir(path):
            index = os.path.basename(path)
            info = index.split('_')
            for key, value in zip(info[1::2], info[2::2]):
                try:
                    val = int(value)
                except valueError:
                    val = float(value)
                output[key] = val
            for re in os.listdir(path):
                re_abs = os.path.join(path, re)
                if '.gz' in re:
                    filename = re.replace('.gz', '')
                    file_abs = os.path.join(path, filename)
                    with gzip.open(re_abs, 'rb') as f_in:
                        with open(file_abs, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    re = filename
                if re in out_names:
                    with open(re_abs, 'rb') as f:
                        blob = f.read()
                    output[re] = blob
                elif '.madx' in re:
                    with open(re_abs, 'rb') as f:
                        blob = f.read()
                    output['madx_in'] = blob
            output['mtime'] = time.time()
        return output

    def create_table(self, table_name, table_info, recreate=False):
        '''Create a new table or recreate an existing table'''
        self.adaptor.create_table(self.conn, table_name, table_info)

    def create_tables(self, tables, recreate=False):
        '''Create multiple tables'''
        for key, value in tables.items():
            self.create_table(key, value, recreate)

    def insert(self, table_name, values):
        '''Insert a row of values'''
        self.adaptor.insert(self.conn, table_name, values)

    def select(self, table_name, where=None, orderby=None, **args):
        '''Select values with specified conditions'''
        self.adaptor.select(self.conn, table_name, where, orderby)

    def remove(self, table_name, **args):
        '''Reomve rows based on specified conditions'''
        pass
