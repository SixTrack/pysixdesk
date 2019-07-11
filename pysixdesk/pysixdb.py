from . import dbadaptor


def SixDB(db_info, settings=None, create=False, log_file=None):
    '''
    Figures out which dbadaptor to use.

    @db_info: dict containing the required database information
    @settings: databse settings, i.e. pragmas
    @create: bool controls whethter to create the db
    @log_file: if provided, dbadaptor logger will write to file at DEBUG level
    '''
    db_info = db_info.copy()
    if 'db_type' not in db_info.keys():
        dbtype = 'sql'
    else:
        dbtype = db_info.pop('db_type')

    if dbtype == 'sql':
        return dbadaptor.SQLDatabaseAdaptor(db_info, settings=settings, create=create, log_file=log_file)
    elif dbtype == 'mysql':
        return dbadaptor.MySQLDatabaseAdaptor(db_info, settings=settings, create=create, log_file=log_file)
    else:
        content = "Unknown database type %s! Must be either 'mysql' or 'sql'" % dbtype
        raise ValueError(content)
