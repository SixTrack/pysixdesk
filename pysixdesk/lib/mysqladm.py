import re
import logging

from . import dbadaptor
from .study import Study


class MysqlAdmin(object):
    '''This class is developed for the administrator to manage the mysql
    database used for pysixdesk at a specified site. In pysixdesk, we create a
    user-based mechanism to manage the massive schemas for studies which
    created by different users.

    In general, one user will have all privileges for schemas created by
    himself/herself automatically, and the privileges for other schemas
    which are not created by the user will be decided by the owner of that
    schema.

    And the user could also use this class to grant the privileges on their own
    schemas to other users.
    '''

    def __init__(self, user, **kargs):
        self._logger = logging.getLogger(__name__)
        self.adaptor = dbadaptor.MySQLDatabaseAdaptor()
        self.db_info = {}
        self._login(user, **kargs)

    def _login(self, user, passwd=None, host=None, port=3306):
        '''Login with admin account. Of course, you can login with non-admin
        account but the operations will be limited'''
        if isinstance(user, Study):
            self.db_info['user'] = str(user.db_info['user'])
            self.db_info['passwd'] = str(user.db_info['passwd'])
            self.db_info['host'] = str(user.db_info['host'])
            self.db_info['port'] = int(user.db_info['port'])
        elif isinstance(user, dict):
            self.db_info['user'] = str(user['user'])
            self.db_info['passwd'] = str(user['passwd'])
            self.db_info['host'] = str(user['host'])
            self.db_info['port'] = int(user['port'])
        elif isinstance(user, str):
            self.db_info['user'] = str(user)
            self.db_info['passwd'] = str(passwd)
            self.db_info['host'] = str(host)
            self.db_info['port'] = int(port)
        self.conn = self.adaptor.new_connection(**self.db_info)

    def create_user(self, username, passwd):
        '''Create a new user with all privileges on the db which prefix
        with username'''
        if not self.check_admin():
            self._logger.warning('Access denied! You are not the administrator!')
            return
        self.adaptor.create_user(self.conn, username, passwd)
        pattern = r"`%s\_%%`.*" % (username)
        self.adaptor.grant(self.conn, username, 'ALL PRIVILEGES', pattern,
                           grant=True)
        # Used for checking the privileges of users
        self.adaptor.grant(self.conn, username, 'SELECT', 'mysql.*')

    def check_user(self, username):
        if not self.check_admin():
            self._logger.warning('Access denied! You are not the administrator!')
            return
        return self.adaptor.check_user(self.conn, username)

    def remove_user(self, username):
        '''Remove an user'''
        if not self.check_admin():
            self._logger.warning('Access denied! You are not the administrator!')
            return
        self.adaptor.remove_user(self.conn, username)

    def show_grants(self, username):
        '''Show the previleges of the specified user'''
        grants = self.adaptor.show_grants(self.conn, username)
        for gra in grants:
            self._logger.info(gra)

    def check_admin(self):
        '''Check if the user is administrator'''
        grants = self.adaptor.show_grants(self.conn, self.db_info['user'])
        for gra in grants:
            if re.match(r'GRANT ALL PRIVILEGES ON \*\.\* TO.+', gra):
                return True
        return False

    def grant_all(self, username, db='*', table='*'):
        '''All Privileges'''
        privs = 'ALL PRIVILEGES'
        self.grant(username, privs, db, table)

    def grant_ro(self, username, db='*', table='*'):
        '''Privileges: select'''
        privs = ['SELECT']
        privs = ','.join(privs)
        self.grant(username, privs, db, table)

    def grant_rw(self, username, db='*', table='*'):
        '''Privileges: select, insert , update'''
        privs = ['SELECT', 'INSERT', 'UPDATE']
        privs = ','.join(privs)
        self.grant(username, privs, db, table)

    def grant_rwd(self, username, db='*', table='*'):
        '''Privileges: select, insert , update, drop'''
        privs = ['SELECT', 'INSERT', 'DROP', 'UPDATE']
        privs = ','.join(privs)
        self.grant(username, privs, db, table)

    def grant(self, username, privs, db='*', table='*'):
        '''Grant privileges'''
        if not self.check_admin() and db == '*':
            db = r"`%s\_%%`" % (self.db_info['user'])
        pattern = db + '.' + table
        self._format_check(pattern)
        self.adaptor.grant(self.conn, username, privs, pattern)

    def revoke(self, username, privs, db='*', table='*'):
        '''Revoke privileges'''
        if not self.check_admin() and db == '*':
            db = r"`%s\_%%`" % (self.db_info['user'])
        pattern = db + '.' + table
        self._format_check(pattern)
        self.adaptor.revoke(self.conn, username, privs, pattern)

    def _format_check(self, pattern):
        '''Check the pattern format'''
        if pattern is None:
            return
        if not isinstance(pattern, str):
            a = type(pattern)
            raise TypeError("'%s' object cannot be interpreted as an string!" %
                            (a.__name__))
        if '.' not in pattern:
            raise Exception("Format error! It sould be like '*.*'")

    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.commit()
            self.conn.close()
