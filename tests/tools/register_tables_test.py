# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from tempfile import NamedTemporaryFile

import mock
import pytest

from schematizer.tools.register_tables import RegisterTables
from schematizer.tools.register_tables import table_info


class TestRegisterTables(object):

    @pytest.fixture
    def example_register_tables_batch(self):
        return RegisterTables()

    @pytest.yield_fixture
    def topology_file(self):
        try:
            f = NamedTemporaryFile()
            f.write(
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
            f.file.flush()
            yield f.name
        finally:
            f.close()

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

    @property
    def query_to_result_map(self):
        return {
            "show tables;": ((self.table_1,), (self.table_2,)),
            "show create table test_table_1;": (
                (self.table_1, self.create_stmt_table_1),
            ),
            "show create table test_table_2;": (
                (self.table_2, self.create_stmt_table_2),
            ),
            "show columns from test_table_1;": self.columns_for_table_1,
            "show columns from test_table_2;": self.columns_for_table_2
        }

    @pytest.fixture
    def connection_param(self):
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
            self.table_1: table_info(
                create_table_stmt=self.create_stmt_table_1,
                columns=['id', 'value']
            ),
            self.table_2: table_info(
                create_table_stmt=self.create_stmt_table_2,
                columns=['id2', 'value2']
            )
        }

    @pytest.fixture
    def insert_query(self):
        return "insert into {} values (23, \"test_value\")".format(
            self.table_1
        )

    @pytest.fixture
    def delete_query(self):
        return "delete from {};".format(self.table_1)

    @pytest.fixture
    def select_query(self):
        return 'show create table {};'.format(self.table_1)

    @pytest.fixture
    def show_columns_query(self):
        return "show columns from {};".format(self.table_1)

    def query_result(self, connection, query):
        if self.query_to_result_map.get(query):
            return self.query_to_result_map.get(query)
        else:
            return []

    def test_get_connection_param_from_topology(
        self,
        topology_file,
        cluster,
        connection_param,
        example_register_tables_batch
    ):
        conn_param = (
            example_register_tables_batch.get_connection_param_from_topology(
                topology_file,
                cluster
            )
        )
        assert conn_param == connection_param

    def test_verify_mysql_table_to_avro_schema(
        self,
        table_to_info_map,
        table_to_avro_fields_map,
        example_register_tables_batch
    ):
        actual_registered_tables, actual_failed_tables = (
            example_register_tables_batch.verify_mysql_table_to_avro_schema(
                table_to_info_map,
                table_to_avro_fields_map
            )
        )
        assert set(actual_registered_tables) == {self.table_1, self.table_2}
        assert set(actual_failed_tables) == set()

    def test_execute_only_whitelisted_queries(
        self,
        insert_query,
        delete_query,
        select_query,
        show_columns_query,
        connection_param,
        example_register_tables_batch
    ):
        with mock.patch(
            'schematizer.tools.register_tables.pymysql.connect'
        ) as mock_connection:
            result = example_register_tables_batch._execute_query(
                mock_connection,
                select_query
            )
            assert result is not None
            result = example_register_tables_batch._execute_query(
                mock_connection,
                show_columns_query
            )
            assert result is not None

        with mock.patch(
            'schematizer.tools.register_tables.pymysql.connect'
        ) as mock_connection:
            result = example_register_tables_batch._execute_query(
                connection_param,
                insert_query
            )
            assert mock_connection.cursor.call_count == 0
            assert result == []
            result = example_register_tables_batch._execute_query(
                connection_param,
                delete_query
            )
            assert mock_connection.cursor.call_count == 0
            assert result == []

    def test_dry_run(
        self,
        topology_file,
        table_to_info_map,
        table_to_avro_fields_map,
        example_register_tables_batch
    ):
        options = mock.Mock(config_file=topology_file, cluster_name='test')
        with mock.patch.object(
            example_register_tables_batch,
            '_execute_query'
        ) as exec_query, mock.patch(
            'schematizer.tools.register_tables.pymysql.connect'
        ), mock.patch.object(
            example_register_tables_batch,
            'verify_mysql_table_to_avro_schema'
        ) as mock_verify_mysql_table:
            example_register_tables_batch.options = options
            mock_verify_mysql_table.return_value = ([], [])
            exec_query.side_effect = self.query_result
            example_register_tables_batch.run()
            mock_verify_mysql_table.assert_called_once_with(
                table_to_info_map,
                table_to_avro_fields_map
            )
