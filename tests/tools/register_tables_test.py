# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import sys

import mock
from schematizer.tools import register_tables
from tempfile import NamedTemporaryFile


class TestRegisterTables(object):

    @pytest.yield_fixture
    def topology_file(self):
        try:
            file = NamedTemporaryFile()
            file.write(
                '''
                topology:
                - cluster: test
                  replica: 'slave'
                  entries:
                    - charset: utf8
                      db: 'yelp_test'
                      host: 10.11.22.333
                      port: 3344
                      passwd: ''
                      use_unicode: true
                      user: 'test_user'
                '''
            )
            file.file.flush()
            yield file.name
        finally:
            file.close()

    @pytest.fixture
    def cluster(self):
        return 'test'

    @property
    def table_1(self):
        return 'test_table_1'

    @property
    def create_stmt_table_1(self):
        return 'create table test_table_1 (id int(11), value varchar(25))'

    @property
    def columns_for_table_1(self):
        return (('id', 'int(11)'), ('value', 'varchar(25)'))

    @property
    def table_2(self):
        return 'test_table_2'

    @property
    def create_stmt_table_2(self):
        return 'create table test_table_2 (id2 int(5), value2 varchar(50))'

    @property
    def columns_for_table_2(self):
        return (('id2', 'int(5)'), ('value2', 'varchar(50)'))

    @pytest.fixture
    def expected_connection_param(self):
        return {
            'charset': 'utf8',
            'db': 'yelp_test',
            'host': '10.11.22.333',
            'passwd': '',
            'port': 3344,
            'use_unicode': True,
            'user': 'test_user',
            'weight': 1.0
        }

    @pytest.fixture
    def table_to_avro_fields_map(self):
        return {
            self.table_1: ['id', 'value'],
            self.table_2: ['id2', 'value2']
        }

    @pytest.fixture
    def table_to_info_map(self):
        return {
            self.table_1: (
                self.create_stmt_table_1,
                ['id', 'value']
            ),
            self.table_2: (
                self.create_stmt_table_2,
                ['id2', 'value2']
            )
        }

    def query_result(self, connection, query):
        if query == "show tables;":
            return (self.table_1,), (self.table_2,)
        elif query == "show create table {};".format(self.table_1):
            return ((self.table_1, self.create_stmt_table_1),)
        elif query == "show create table {};".format(self.table_2):
            return ((self.table_2, self.create_stmt_table_2),)
        elif query == "show columns from {};".format(self.table_1):
            return self.columns_for_table_1
        elif query == "show columns from {};".format(self.table_2):
            return self.columns_for_table_2
        else:
            return []

    def test_get_connection_param_from_topology(
        self,
        topology_file,
        cluster,
        expected_connection_param
    ):
        conn_param = register_tables.get_connection_param_from_topology(topology_file, cluster)
        assert conn_param == expected_connection_param

    def test_verify_mysql_table_to_avro_schema(
        self,
        table_to_info_map,
        table_to_avro_fields_map
    ):
        actual_registered_tables, actual_failed_tables = (
            register_tables.verify_mysql_table_to_avro_schema(
                table_to_info_map,
                table_to_avro_fields_map
            )
        )
        assert set(actual_registered_tables) == {self.table_1, self.table_2}
        assert set(actual_failed_tables) == set()

    def test_main(
        self,
        topology_file,
        table_to_info_map,
        table_to_avro_fields_map
    ):
        testargs = ['schematizer/tools/register_tables.py', '-c', 'test', '--proddb']
        with mock.patch.object(sys, 'argv', testargs), mock.patch(
                'schematizer.tools.register_tables.get_topology_file'
        ) as mock_get_topology_file, mock.patch(
            'schematizer.tools.register_tables._execute_query'
        ) as exec_query, mock.patch(
            'schematizer.tools.register_tables.pymysql.connect'
        ) as mock_connect, mock.patch(
            'schematizer.tools.register_tables.verify_mysql_table_to_avro_schema'
        ) as mock_verify_mysql_table :
            mock_verify_mysql_table.return_value = ([],[])
            mock_connect.return_value = mock.Mock()
            mock_get_topology_file.return_value = topology_file
            exec_query.side_effect = self.query_result
            register_tables.main()
            mock_verify_mysql_table.assert_called_once_with(
                table_to_info_map,
                table_to_avro_fields_map
            )
