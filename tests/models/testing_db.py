# -*- coding: utf-8 -*-
# This is shamelessly copied from
# checkout_fulfillment servicve with some minor changes.
import atexit
import contextlib
from glob import glob

import pytest
import staticconf.testing
import yelp_conn
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker as sessionmaker_sa
from yelp_conn.testing import sandbox
from yelp_lib.classutil import cached_property

from schematizer.models import database


DB_NAME = 'schematizer'


class PerProcessMySQLDaemon(object):

    _db_name = DB_NAME

    def __init__(self):
        self._mysql_sandbox_context = sandbox.start()
        self._create_database()
        self._create_tables()

        atexit.register(self.clean_up)

    def _create_tables(self):
        sandbox.load_fixtures(
            self._mysql_daemon.get_conn(),
            self._db_name,
            glob('schema/tables/*.sql')
        )

    def truncate_all_tables(self):
        self._session.execute('begin')
        for table in self._all_tables:
            was_modified = self._session.execute(
                "select count(*) from `%s` limit 1" % table
            ).scalar()
            if was_modified:
                self._session.execute('truncate table `%s`' % table)
        self._session.execute('commit')

    def clean_up(self):
        self._mysql_sandbox_context.__exit__(None, None, None)

    @cached_property
    def engine(self):
        return create_engine(self._url)

    @cached_property
    def _make_session(self):
        # regular sqlalchemy session maker
        return sessionmaker_sa(bind=self.engine)

    def _create_database(self):
        conn = self._engine_without_db.connect()
        conn.execute('create database ' + self._db_name)
        conn.close()

    @cached_property
    def _session(self):
        return self._make_session()

    @property
    def _url(self):
        return 'mysql://localhost/%s?unix_socket=%s' % (
            self._db_name,
            self._mysql_daemon.socket
        )

    @property
    def _engine_without_db(self):
        return create_engine(self._url_without_db)

    @property
    def _url_without_db(self):
        return 'mysql://localhost/?unix_socket=%s' % (
               self._mysql_daemon.socket
        )

    @cached_property
    def _mysql_daemon(self):
        return self._mysql_sandbox_context.__enter__()

    @property
    def _all_tables(self):
        return self.engine.table_names()


class DBTestCase(object):

    _per_process_mysql_daemon = PerProcessMySQLDaemon()

    @property
    def engine(self):
        return self._per_process_mysql_daemon.engine

    @pytest.yield_fixture(autouse=True)
    def sandboxed_session(self):
        self._session_prev_engine = database.session.bind

        with setup_yelp_conn_topology(self.engine):
            database.session.bind = self.engine
            database.session.enforce_read_only = False
            yield database.session
        database.session.bind = self._session_prev_engine

    @pytest.yield_fixture(autouse=True)
    def rollback_session_after_test(self, sandboxed_session):
        """After each test, rolls back the sandboxed_session"""
        yield
        sandboxed_session.rollback()

    @pytest.yield_fixture(autouse=True)
    def _truncate_all_tables(self):
        try:
            yield
        except:
            pass
        finally:
            self._per_process_mysql_daemon.truncate_all_tables()


@contextlib.contextmanager
def setup_yelp_conn_topology(engine):
    """Point yelp_conn topology configs to a mysql test db engine"""
    yelp_conn.reset_module()
    mysql_url = engine.url

    topology = TopologyFactory.make(
        cluster='schematizer',
        db=mysql_url.database,
        user=(mysql_url.username or ''),
        passwd=(mysql_url.password or ''),
        unix_socket=mysql_url.query['unix_socket'])
    topology_file = yelp_conn.parse_topology(topology)

    # Initialize yelp_conn if we haven't already, then replace the
    # connection sets to dev db with connection set to test db
    mock_conf = {
        'topology': topology_file,
        'connection_set_file': './connection_sets.yaml'
    }
    with staticconf.testing.MockConfiguration(
            mock_conf,
            namespace=yelp_conn.config.namespace
    ):
        yelp_conn.initialize()
        yield


class TopologyFactory(object):

    """Create a yelp_conn topology that can be used with
    yelp_conn.parse_topology() and yelp_conn.load_topology()

    Use this to create a yelp_conn topology config that points to your
    mysql test database.
    """

    @classmethod
    def make(cls, cluster, db, unix_socket, user='', passwd=''):
        common_settings = {
            'cluster': cluster,
            'db': db,
            'user': user,
            'passwd': passwd,
            'unix_socket': unix_socket
        }
        entries = [
            cls._entry(**dict(common_settings, replica='master')),
            cls._entry(**dict(common_settings, replica='slave')),
            cls._entry(**dict(common_settings, replica='reporting')),
        ]
        return {'topology': entries}

    @staticmethod
    def _entry(cluster, replica, db, user, passwd, unix_socket):
        return {
            'cluster': cluster,
            'replica': replica,
            'entries': [{
                'unix_socket': unix_socket,
                'user': user,
                'passwd': passwd,
                'db': db
            }]
        }
