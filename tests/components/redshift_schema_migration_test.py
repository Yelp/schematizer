# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.components.converters import AvroToRedshiftConverter
from schematizer.components.redshift_schema_migration \
    import RedshiftSchemaMigration
from schematizer.models import redshift_data_types as data_types
from schematizer.models.sql_entities import DbPermission
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLAttribute
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class TestRedshiftSchemaMigration(object):

    @pytest.fixture
    def converter(self):
        return AvroToRedshiftConverter()

    @pytest.fixture
    def migration(self):
        return RedshiftSchemaMigration()

    @property
    def table_name(self):
        return 'foo_table'

    @property
    def schema_name(self):
        return 'foo_schema'

    @property
    def another_table_name(self):
        return 'bar_table'

    @property
    def same_col(self):
        return SQLColumn('same_col', data_types.RedshiftVarChar(64))

    @property
    def old_col(self):
        return SQLColumn(
            'old_col',
            data_types.RedshiftDecimal(precision=4, scale=2)
        )

    @property
    def renamed_col(self):
        return SQLColumn(
            'renamed_col',
            self.old_col.type,
            aliases=self.old_col.name
        )

    @property
    def random_col(self):
        return SQLColumn('col', data_types.RedshiftInteger())

    @property
    def old_table(self):
        columns = [self.same_col, self.old_col, self.random_col]
        return SQLTable(
            self.table_name,
            columns,
            schema_name=self.schema_name
        )

    @property
    def another_old_table(self):
        columns = [self.same_col, self.old_col, self.random_col]
        return SQLTable(
            self.another_table_name,
            columns,
            schema_name=self.schema_name
        )

    @property
    def primary_key_column(self):
        return SQLColumn(
            'primary_key_col',
            data_types.RedshiftInteger(),
            primary_key_order=1
        )

    @property
    def second_primary_key_column(self):
        return SQLColumn(
            'second_pkey_col',
            data_types.RedshiftInteger(),
            primary_key_order=2
        )

    @property
    def double_precision_column(self):
        return SQLColumn(
            'double_precision_col',
            data_types.RedshiftDouble()
        )

    @property
    def real_column(self):
        return SQLColumn(
            'real_col',
            data_types.RedshiftReal()
        )

    @property
    def permission_one(self):
        return DbPermission(self.table_name, 'admin', 'ALL', for_group=True)

    @property
    def expected_permission_one(self):
        return 'GRANT ALL ON {0} TO GROUP admin;'.format(self.table_name)

    @property
    def permission_two(self):
        return DbPermission(self.table_name, 'users', 'SELECT', for_group=True)

    @property
    def expected_permission_two(self):
        return 'GRANT SELECT ON {0} TO GROUP users;'.format(self.table_name)

    @property
    def new_table(self):
        columns = [self.same_col, self.renamed_col, self.primary_key_column]
        table = SQLTable(
            self.table_name,
            columns,
            schema_name=self.schema_name
        )
        table.metadata[MetaDataKey.PERMISSION] = [self.permission_one,
                                                  self.permission_two]
        return table

    @property
    def expected_new_schema_sql(self):
        return 'CREATE SCHEMA IF NOT EXISTS {}'.format(
            self.new_table.schema_name
        )

    @property
    def expected_new_table_sql(self):
        return (
            'CREATE TABLE {0} ({1} varchar(64),{2} decimal(4,2),'
            '{3} integer,PRIMARY KEY ({3}));'.format(
                self.new_table.full_name,
                self.same_col.name,
                self.renamed_col.name,
                self.primary_key_column.name
            )
        )

    def test_create_simple_push_plan_with_simple_table(self, migration):
        simple_table = SQLTable(self.table_name, [self.same_col])
        actual = migration.create_simple_push_plan(simple_table)
        expected = [
            'BEGIN;',
            'CREATE TABLE {} (same_col varchar(64));'.format(
                simple_table.full_name
            ),
            '',
            'COMMIT;'
        ]
        assert expected == actual

    def test_create_simple_push_plan_with_new_table(self, migration):
        insert_sql = ''
        expected = [
            'BEGIN;',
            self.expected_new_schema_sql,
            self.expected_new_table_sql,
            insert_sql,
            self.expected_permission_one,
            self.expected_permission_two,
            'COMMIT;'
        ]
        actual = migration.create_simple_push_plan(self.new_table)
        assert expected == actual

    def test_create_simple_push_plan_with_different_name(self, migration):
        insert_sql = ('INSERT INTO {0} ({1}) (SELECT {2} FROM {3});'.format(
            self.new_table.full_name,
            ', '.join([self.same_col.name, self.renamed_col.name]),
            ', '.join([self.same_col.name, self.old_col.name]),
            self.another_old_table.full_name
        ))
        expected = [
            'BEGIN;',
            self.expected_new_schema_sql,
            self.expected_new_table_sql,
            insert_sql,
            self.expected_permission_one,
            self.expected_permission_two,
            'COMMIT;'
        ]
        actual = migration.create_simple_push_plan(
            self.new_table,
            self.another_old_table
        )
        assert expected == actual

    def test_create_simple_push_plan_with_existing_table(self, migration):
        temp_table_full_name = '{0}.{1}_tmp'.format(
            self.schema_name,
            self.new_table.name
        )
        create_sql = self.expected_new_table_sql.replace(
            self.new_table.full_name,
            temp_table_full_name
        )

        insert_sql = ('INSERT INTO {0} ({1}) (SELECT {2} FROM {3});'.format(
            temp_table_full_name,
            ', '.join([self.same_col.name, self.renamed_col.name]),
            ', '.join([self.same_col.name, self.old_col.name]),
            self.old_table.full_name
        ))

        old_table_name = self.new_table.name + '_old'
        old_table_full_name = self.new_table.full_name + '_old'

        expected = [
            'BEGIN;',
            self.expected_new_schema_sql,
            create_sql,
            insert_sql,
            'ALTER TABLE {0} RENAME TO "{1}";'.format(
                self.new_table.full_name,
                old_table_name
            ),
            'ALTER TABLE {0} RENAME TO "{1}";'.format(
                temp_table_full_name,
                self.new_table.name
            ),
            self.expected_permission_one,
            self.expected_permission_two,
            'DROP TABLE {0};'.format(old_table_full_name),
            'COMMIT;'
        ]
        actual = migration.create_simple_push_plan(
            self.new_table,
            self.old_table
        )
        assert expected == actual

    def test_get_column_def_sql(self, migration):
        column = SQLColumn('foo', data_types.RedshiftInteger())
        expected = 'foo integer'
        actual = migration.get_column_def_sql(column)
        assert expected == actual

    def test_get_column_def_sql_with_attributes(self, migration):
        attributes = [SQLAttribute('attr_one')]
        column = SQLColumn(
            'foo',
            data_types.RedshiftInteger(),
            is_nullable=False,
            default_value='',
            attributes=attributes,
            aliases='bar'
        )
        expected = 'foo integer not null default \'\' attr_one'
        actual = migration.get_column_def_sql(column)
        assert expected == actual

    def test_get_column_def_sql_with_string_type(self, migration):
        column = SQLColumn(
            'foo',
            data_types.RedshiftVarChar(256),
            attributes=[SQLAttribute.create_with_value('attr', '')],
            aliases='bar'
        )
        expected = 'foo varchar(256) attr \'\''
        actual = migration.get_column_def_sql(column)
        assert expected == actual

    def test_concatenate_attributes(self, migration):
        attributes = [
            SQLAttribute('attr1'),
            SQLAttribute.create_with_value('attr2', value=''),
            SQLAttribute.create_with_value('attr3', value=None),
            SQLAttribute.create_with_value('attr4', value=1.2),
            SQLAttribute.create_with_value('attr5', value="let's test")
        ]
        expected = "attr1 attr2 '' attr3 null attr4 1.2 attr5 'let\\'s test'"
        actual = migration.concatenate_attributes(attributes)
        assert expected == actual

    def test_insert_table_sql_with_no_src_table(self, migration):
        actual = migration.insert_table_sql(self.new_table)
        assert '' == actual

    def test_get_primary_key_sql(self, migration):
        expected = 'PRIMARY KEY ({0})'.format(self.primary_key_column.name)
        actual = migration.get_primary_key_sql(self.new_table)
        assert expected == actual

    def test_get_primary_key_sql_with_no_primary_key(self, migration):
        actual = migration.get_primary_key_sql(self.old_table)
        assert '' == actual

    def test_rename_table_sql(self, migration):
        expected = 'ALTER TABLE foo RENAME TO "bar";'
        actual = migration.rename_table_sql('foo', 'bar')
        assert expected == actual

    def test_grant_permission_sql_for_user(self, migration):
        permission = DbPermission('foo', 'bob', 'select')
        expected = 'GRANT select ON foo TO bob;'
        actual = migration.grant_permission_sql(permission)
        assert expected == actual

    def test_grant_permission_sql_for_group(self, migration):
        permission = DbPermission('foo', 'admins', 'all', for_group=True)
        expected = 'GRANT all ON foo TO GROUP admins;'
        actual = migration.grant_permission_sql(permission)
        assert expected == actual

    def test_drop_table_sql(self, migration):
        assert 'DROP TABLE foo;' == migration.drop_table_sql('foo')

    def test_create_table_sql_with_no_primary_key(self, migration):
        expected = (
            'CREATE TABLE {0} ({1} varchar(64),{2} decimal(4,2),'
            '{3} integer);'.format(
                self.old_table.full_name,
                self.same_col.name,
                self.old_col.name,
                self.random_col.name
            )
        )
        actual = migration.create_table_sql(self.old_table)
        assert expected == actual

    def test_create_table_sql_with_composite_primary_keys(self, migration):
        columns = [self.second_primary_key_column, self.primary_key_column]
        table = SQLTable(self.table_name, columns)
        actual = migration.create_table_sql(table)

        expected = (
            'CREATE TABLE {0} ({1} integer,{2} integer,PRIMARY KEY ({3}));'
            .format(
                self.table_name,
                self.second_primary_key_column.name,
                self.primary_key_column.name,
                ', '.join([self.primary_key_column.name,
                           self.second_primary_key_column.name])
            )
        )
        assert expected == actual

    def test_create_table_sql_with_real_number_col_types(self, migration):
        columns = [self.double_precision_column, self.real_column]
        table = SQLTable(self.table_name, columns)
        actual = migration.create_table_sql(table)

        expected = (
            'CREATE TABLE {0} ({1} double precision,{2} real);'
            .format(
                self.table_name,
                self.double_precision_column.name,
                self.real_column.name
            )
        )
        assert expected == actual

    def test_all_redshift_data_types(self, converter, migration):
        all_types_record_schema = {
            'type': 'record',
            'name': 'all_types',
            'namespace': None,
            'fields': [
                # NUMERIC TYPES

                # Unsupported:
                # smallint - TODO(DATAPIPE-1520)

                # Supported:
                {'name': 'integer_col', 'type': 'int'},
                {'name': 'integer_col_default', 'type': ['null', 'int'],
                    'default': 10},
                {'name': 'integer_col_nullable', 'type': ['null', 'int'],
                    'default': None},
                {'name': 'bigint_col', 'type': ['null', 'long'],
                    'default': None},
                {'name': 'decimal_col', 'type': ['null', {
                    'logicalType': 'decimal',
                    'precision': 4, 'scale': 2, 'type': 'bytes'}],
                    'default': None},
                {'name': 'real_col', 'type': ['null', 'float'],
                    'default': None},
                {'name': 'double_precision_col', 'type': ['null', 'double'],
                    'default': None},


                # CHARACTER TYPES

                # Unsupported:
                # char - deliberate since it does not support multibyte chars
                # bpchar - equivalent to CHAR(256), which is unsupported
                # text - equivalent to VARCHAR(256)

                # Supported:
                {'name': 'varchar_col_fixlen', 'type': ['null', 'string'],
                    'fixlen': 256},
                {'name': 'varchar_col_maxlen', 'type': ['null', 'string'],
                    'maxlen': 256},
                {'name': 'varchar_col_maxsize', 'type': ['null', 'string'],
                    'maxlen': 99999},
                {'name': 'varchar_col_enum', 'type': {
                    'type': 'enum', 'name': 'varchar_col_enum',
                    'symbols': ['a', 'abc', 'ab']}
                 },


                # DATETYPE TYPES

                # Supported:
                {'name': 'date_col', 'type': ['null', {
                    'logicalType': 'date', 'type': 'int'}]
                 },
                {'name': 'timestamp_col', 'type': ['null', 'long'],
                    'timestamp': True},


                # BOOLEAN TYPES

                # Supported
                {'name': 'boolean_col', 'type': ['null', 'boolean']},


            ]
        }
        create_table_stmt = migration.create_table_sql(
            converter.convert(all_types_record_schema)
        )
        assert create_table_stmt == (
            'CREATE TABLE all_types ('
            'integer_col integer not null,'
            'integer_col_default integer default 10,'
            'integer_col_nullable integer,'
            'bigint_col bigint,'
            'decimal_col decimal(4,2),'
            'real_col real,'
            'double_precision_col double precision,'
            'varchar_col_fixlen varchar(256),'
            'varchar_col_maxlen varchar(512),'
            'varchar_col_maxsize varchar(65535),'
            'varchar_col_enum varchar(3) not null,'
            'date_col date,'
            'timestamp_col timestamp,'
            'boolean_col boolean'
            ');'
        )
