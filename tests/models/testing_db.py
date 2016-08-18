# -*- coding: utf-8 -*-
# This is shamelessly copied from
# checkout_fulfillment servicve with some minor changes.
from __future__ import absolute_import
from __future__ import unicode_literals

import atexit
from glob import glob

import pytest
from cached_property import cached_property
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker as sessionmaker_sa

import testing.mysqld
from schematizer.models import database


# Generate Mysqld class which shares the generated database
Mysqld = testing.mysqld.MysqldFactory(cache_initialized_db=True)


class PerProcessMySQLDaemon(object):

    _db_name = 'schematizer'

    def __init__(self):
        self._mysql_daemon = Mysqld()
        self._create_database()
        self._create_tables()

        atexit.register(self.clean_up)

    def _create_tables(self):
        fixtures = glob('schema/tables/*.sql')
        conn = self.engine.connect()
        with conn.begin():
            conn.execute('use %s' % self._db_name)
            for fixture in fixtures:
                with open(fixture, 'r') as fh:
                    conn.execute(fh.read())

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
        self._mysql_daemon.stop()

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
        return self._mysql_daemon.url(db=self._db_name)

    @property
    def _engine_without_db(self):
        return create_engine(self._url_without_db)

    @property
    def _url_without_db(self):
        return self._mysql_daemon.url()

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
