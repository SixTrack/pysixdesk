from collections import OrderedDict

from . import dbtypedict

class Table(object):
    '''This class aims to initialize and customize the database tables'''

    def __init__(self, tables, table_keys, db_type):
        self.tables = tables
        self.table_keys = table_keys
        self.db_type = db_type
        if db_type.lower() == 'sql':
            self.type_dict = dbtypedict.SQLiteDict()
        elif db_type.lower() == 'mysql':
            self.type_dict = dbtypedict.MySQLDict()
        self.tables['templates'] = OrderedDict()
        self.tables['env'] = OrderedDict()
        self.tables['boinc_vars'] = OrderedDict()
        self.init_preprocess_tables()
        self.init_sixtrack_tables()

    @staticmethod
    def result_table(filelist):
        '''Retrun the map between the result files and database tables
        fileName --> tableName'''
        relation = {}
        relation['fort.10'] = 'six_results'
        relation['aperture_losses.dat'] = 'aperture_losses'
        relation['oneturnresult'] = 'oneturn_sixtrack_results'
        relation['Coll_Scatter.dat'] = 'collimation_losses'
        relation['final_state.dat'] = 'final_state'
        relation['initial_state.dat'] = 'init_state'

        filemap = {}
        for fil in filelist:
            if fil in relation.keys():
                filemap[fil] = relation[fil]
            else:
                filemap[fil] = None
        return filemap

    def customize_tables(self, table_names, info, dtype=None):
        '''Customize the tables'''
        if isinstance(info, dict):
            for key, val in info.items():
                self.tables[table_names][key] = self.type_dict[val]
        elif isinstance(info, list):
            if dtype is None:
                raise Exception("Data type information needed!")
            for key in info:
                self.tables[table_names][key] = dtype
        else:
            raise Exception("Unsupported input format!")

    def init_preprocess_tables(self):
        self.tables['preprocess_wu'] = OrderedDict([
            ('wu_id', 'INTEGER'),
            ('job_name', 'text'),
            ('input_file', 'blob'),
            ('batch_name', 'text'),
            ('unique_id', 'text'),
            ('status', 'text'),
            ('task_id', 'int'),
            ('mtime', 'bigint')])
        self.table_keys['preprocess_wu'] = {
            'primary': ['wu_id'],
            'autoincrement': ['wu_id'],
            'foreign': {},
        }
        self.tables['preprocess_task'] = OrderedDict([
            ('task_id', 'INTEGER'),
            ('wu_id', 'int'),
            ('madx_in', 'blob'),
            ('madx_stdout', 'blob'),
            ('job_stdout', 'blob'),
            ('job_stderr', 'blob'),
            ('job_stdlog', 'blob'),
            ('status', 'text'),
            ('mtime', 'bigint')])
        self.table_keys['preprocess_task'] = {
            'primary': ['task_id'],
            'autoincrement': ['task_id'],
            'foreign': {'preprocess_wu': [['wu_id'], ['wu_id']]},
        }

    def init_sixtrack_tables(self):
        self.tables['sixtrack_wu'] = OrderedDict([
            ('wu_id', 'INTEGER'),
            ('preprocess_id', 'int'),
            ('job_name', 'text'),
            ('input_file', 'blob'),
            ('batch_name', 'text'),
            ('unique_id', 'text'),
            ('status', 'text'),
            ('task_id', 'int'),
            ('boinc', 'text'),
            ('mtime', 'bigint')])
        self.table_keys['sixtrack_wu'] = {
            'primary': ['wu_id'],
            'autoincrement': ['wu_id'],
            'foreign': {'preprocess_wu': [['preprocess_id'], ['wu_id']]},
        }
        self.tables['sixtrack_task'] = OrderedDict([
            ('task_id', 'INTEGER'),
            ('wu_id', 'int'),
            ('fort3', 'blob'),
            ('job_stdout', 'blob'),
            ('job_stderr', 'blob'),
            ('job_stdlog', 'blob'),
            ('status', 'text'),
            ('mtime', 'bigint')])
        self.table_keys['sixtrack_task'] = {
            'primary': ['task_id'],
            'autoincrement': ['task_id'],
            'foreign': {'sixtrack_wu': [['wu_id'], ['wu_id']]},
        }
        self.tables['six_results'] = OrderedDict([
            ('task_id', 'int'),
            ('row_num', 'int'),
            ('turn_max', 'int'),
            ('sflag', 'int'),
            ('qx', 'float'),
            ('qy', 'float'),
            ('betx', 'float'),
            ('bety', 'float'),
            ('sigx1', 'float'),
            ('sigy1', 'float'),
            ('deltap', 'float'),
            ('dist', 'float'),
            ('distp', 'float'),
            ('qx_det', 'float'),
            ('qx_spread', 'float'),
            ('qy_det', 'float'),
            ('qy_spread', 'float'),
            ('resxfact', 'float'),
            ('resyfact', 'float'),
            ('resorder', 'int'),
            ('smearx', 'float'),
            ('smeary', 'float'),
            ('smeart', 'float'),
            ('sturns1', 'int'),
            ('sturns2', 'int'),
            ('sseed', 'float'),
            ('qs', 'float'),
            ('sigx2', 'float'),
            ('sigy2', 'float'),
            ('sigxmin', 'float'),
            ('sigxavg', 'float'),
            ('sigxmax', 'float'),
            ('sigymin', 'float'),
            ('sigyavg', 'float'),
            ('sigymax', 'float'),
            ('sigxminld', 'float'),
            ('sigxavgld', 'float'),
            ('sigxmaxld', 'float'),
            ('sigyminld', 'float'),
            ('sigyavgld', 'float'),
            ('sigymaxld', 'float'),
            ('sigxminnld', 'float'),
            ('sigxavgnld', 'float'),
            ('sigxmaxnld', 'float'),
            ('sigyminnld', 'float'),
            ('sigyavgnld', 'float'),
            ('sigymaxnld', 'float'),
            ('emitx', 'float'),
            ('emity', 'float'),
            ('betx2', 'float'),
            ('bety2', 'float'),
            ('qpx', 'float'),
            ('qpy', 'float'),
            ('version', 'float'),
            ('cx', 'float'),
            ('cy', 'float'),
            ('csigma', 'float'),
            ('xp', 'float'),
            ('yp', 'float'),
            ('delta', 'float'),
            ('dnms', 'float'),
            ('trttime', 'float'),
            ('mtime', 'bigint')])
        self.table_keys['six_results'] = {
            'primary': ['task_id', 'row_num'],
            'foreign': {'sixtrack_task': [['task_id'], ['task_id']]},
        }

    def init_oneturn_tables(self):
        self.tables['oneturn_sixtrack_wu'] = OrderedDict()
        self.tables['oneturn_sixtrack_results'] = OrderedDict([
            ('task_id', 'int'),
            ('row_num', 'int'),
            ('wu_id', 'int'),
            ('betax', 'float'),
            ('betax2', 'float'),
            ('betay', 'float'),
            ('betay2', 'float'),
            ('tunex', 'float'),
            ('tuney', 'float'),
            ('chromx', 'float'),
            ('chromy', 'float'),
            ('x', 'float'),
            ('xp', 'float'),
            ('y', 'float'),
            ('yp', 'float'),
            ('z', 'float'),
            ('zp', 'float'),
            ('chromx_s', 'float'),
            ('chromy_s', 'float'),
            ('chrom_eps', 'float'),
            ('tunex1', 'float'),
            ('tuney1', 'float'),
            ('tunex2', 'float'),
            ('tuney2', 'float'),
            ('mtime', 'bigint')])

    def init_collimation_tables(self):
        self.tables['aperture_losses'] = OrderedDict([
            ('task_id', 'int'),
            ('row_num', 'int'),
            ('turn', 'int'),
            ('block', 'int'),
            ('bezid', 'int'),
            ('bez', 'text'),
            ('slos', 'float'),
            ('fluka_uid', 'int'),
            ('fluka_gen', 'int'),
            ('fluka_weight', 'float'),
            ('x', 'float'),
            ('xp', 'float'),
            ('y', 'float'),
            ('yp', 'float'),
            ('etot', 'float'),
            ('dE', 'float'),
            ('dT', 'float'),
            ('A_atom', 'int'),
            ('Z_atom', 'int'),
            ('mtime', 'bigint')])
        self.table_keys['aperture_losses'] = {
            'primary': ['task_id', 'row_num'],
            'foreign': {'sixtrack_task': [['task_id'], ['task_id']]},
        }
        self.tables['collimation_losses'] = OrderedDict([
            ('task_id', 'int'),
            ('row_num', 'int'),
            ('icoll', 'int'),
            ('iturn', 'int'),
            ('np', 'int'),
            ('nabs', 'int'),
            ('dp', 'float'),
            ('dxp', 'float'),
            ('dyp', 'float'),
            ('mtime', 'bigint')])
        self.table_keys['collimation_losses'] = {
            'primary': ['task_id', 'row_num'],
            'foreign': {'sixtrack_task': [['task_id'], ['task_id']]},
        }

    def init_state_tables(self):
        self.tables['init_state'] = OrderedDict([
            ('task_id', 'int'),
            ('row_num', 'int'),
            ('part_id', 'int'),
            ('parent_id', 'int'),
            ('lost', 'text'),
            ('x', 'float'),
            ('y', 'float'),
            ('xp', 'float'),
            ('yp', 'float'),
            ('sigma', 'float'),
            ('dp', 'float'),
            ('p', 'float'),
            ('e', 'float'),
            ('mtime', 'bigint')])
        self.table_keys['init_state'] = {
            'primary': ['task_id', 'row_num'],
            'foreign': {'sixtrack_task': [['task_id'], ['task_id']]},
        }
        self.tables['final_state'] = OrderedDict([
            ('task_id', 'int'),
            ('row_num', 'int'),
            ('part_id', 'int'),
            ('parent_id', 'int'),
            ('lost', 'text'),
            ('x', 'float'),
            ('y', 'float'),
            ('xp', 'float'),
            ('yp', 'float'),
            ('sigma', 'float'),
            ('dp', 'float'),
            ('p', 'float'),
            ('e', 'float'),
            ('mtime', 'bigint')])
        self.table_keys['final_state'] = {
            'primary': ['task_id', 'row_num'],
            'foreign': {'sixtrack_task': [['task_id'], ['task_id']]},
        }
