import sys
import utils
import dbadaptor


def SixDB(db_info, settings=None, create=False, mes_level=1, log_file=None):
    '''
    Figures out which dbadaptor to use.

    @db_info: dict containing the required database information
    @settings: databse settings, i.e. pragmas
    @create: bool controls whethter to create the db
    @mes_level: controls the messaging level, see utils.message
    @log_file: if provided, the utils.message write to provided file
    '''
    db_info = db_info.copy()
    if 'db_type' not in db_info.keys():
        dbtype = 'sql'
    else:
        dbtype = db_info.pop('db_type')

    if dbtype == 'sql':
        return dbadaptor.SQLDatabaseAdaptor(db_info, settings=settings, create=create,
                                            mes_level=mes_level, log_file=log_file)
    elif dbtype == 'mysql':
        return dbadaptor.MySQLDatabaseAdaptor(db_info, settings=settings, create=create,
                                            mes_level=mes_level, log_file=log_file)
    else:
        content = "Unknown database type %s!" % dbtype
        utils.message('Error', content, mes_level, log_file)
        sys.exit(1)
