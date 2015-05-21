# -*- coding: utf-8 -*-
import mock
import pytest

from schematizer.components.redshift_schema_migration \
    import RedshiftSchemaMigration
from schematizer.models import redshift_data_types as data_types
from schematizer.models.redshift_data_types import DbPermission
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLAttribute
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class TestRedshiftSchemaMigration(object):

    @pytest.fixture
    def migration(self):
        return RedshiftSchemaMigration()

    @property
    def table_name(self):
        return 'foo_table'

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
        table = SQLTable(self.table_name)
        columns = [self.same_col, self.old_col, self.random_col]
        table.columns.extend(columns)
        return table

    @property
    def another_old_table(self):
        table = SQLTable(self.another_table_name)
        table.columns.extend(self.old_table.columns)
        return table

    @property
    def primary_key_column(self):
        return SQLColumn(
            'primary_key_col',
            data_types.RedshiftInteger(),
            is_primary_key=True
        )

    @property
    def permissions(self):
        return [
            data_types.DbPermission(self.table_name, True, 'admin', 'ALL'),
            data_types.DbPermission(
                self.table_name,
                True,
                'dw_users',
                'SELECT'
            )
        ]

    @property
    def new_table(self):
        table = SQLTable(self.table_name)
        columns = [self.same_col, self.renamed_col, self.primary_key_column]
        table.columns.extend(columns)
        table.metadata[MetaDataKey.PERMISSION] = self.permissions
        return table

    def test_get_column_def_sql(self, migration):
        column = SQLColumn('foo', data_types.RedshiftInteger())
        expected = 'foo integer'
        actual = migration.get_column_def_sql(column)
        assert expected == actual

    def test_get_column_def_sql_with_attributes(self, migration):
        attributes = [SQLAttribute('attr_one', None, False)]
        column = SQLColumn(
            'foo',
            data_types.RedshiftInteger(),
            is_primary_key=False,
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
            is_primary_key=False,
            attributes=[SQLAttribute('attr', '', True)],
            aliases='bar'
        )
        expected = 'foo varchar(256) attr \'\''
        actual = migration.get_column_def_sql(column)
        assert expected == actual

    def test_concatenate_attributes(self, migration):
        attributes = [
            SQLAttribute('attr1', None, False),
            SQLAttribute('attr2', '', True),
            SQLAttribute('attr3', None, True),
            SQLAttribute('attr4', 1.2, True),
            SQLAttribute('attr5', "let's test", True)
        ]
        expected = "attr1 attr2 '' attr3 null attr4 1.2 attr5 'let\\'s test'"
        actual = migration.concatenate_attributes(attributes)
        assert expected == actual

    def test_insert_table_sql_with_no_src_table(self, migration):
        actual = migration.insert_table_sql(self.new_table)
        assert '' == actual

    def test_insert_table_sql(self, migration):
        expected = ('INSERT INTO {0} ({1}) (SELECT {2} FROM {3});'.format(
            self.new_table.name,
            ', '.join([self.same_col.name, self.renamed_col.name]),
            ', '.join([self.same_col.name, self.old_col.name]),
            self.another_old_table.name
        ))
        actual = migration.insert_table_sql(
            self.new_table,
            self.another_old_table
        )
        assert expected == actual

    def test_get_primary_key_sql(self, migration):
        expected = 'PRIMARY KEY ({0})'.format(self.primary_key_column.name)
        actual = migration.get_primary_key_sql(self.new_table)
        assert expected == actual

    def test_get_primary_key_sql_with_no_primary_key(self, migration):
        actual = migration.get_primary_key_sql(self.old_table)
        assert '' == actual

    def test_rename_table_sql(self, migration):
        expected = 'ALTER TABLE foo RENAME TO bar;'
        actual = migration.rename_table_sql('foo', 'bar')
        assert expected == actual

    def test_grant_permission_sql_for_user(self, migration):
        permission = DbPermission('foo', False, 'bob', 'select')
        expected = 'GRANT select ON foo TO bob;'
        actual = migration.grant_permission_sql(permission)
        assert expected == actual

    def test_grant_permission_sql_for_group(self, migration):
        permission = DbPermission('foo', True, 'admins', 'all')
        expected = 'GRANT all ON foo TO GROUP admins;'
        actual = migration.grant_permission_sql(permission)
        assert expected == actual

    def test_drop_table_sql(self, migration):
        assert 'DROP TABLE foo;' == migration.drop_table_sql('foo')

    def test_create_table_sql(self, migration):
        expected = (
            'CREATE TABLE {0} ({1} varchar(64),{2} decimal(4,2),'
            '{3} integer,PRIMARY KEY ({3}));'.format(
                self.new_table.name,
                self.same_col.name,
                self.renamed_col.name,
                self.primary_key_column.name
            )
        )
        actual = migration.create_table_sql(self.new_table)
        assert expected == actual

    def test_create_table_sql_with_no_primary_key(self, migration):
        expected = (
            'CREATE TABLE {0} ({1} varchar(64),{2} decimal(4,2),'
            '{3} integer);'.format(
                self.old_table.name,
                self.same_col.name,
                self.old_col.name,
                self.random_col.name
            )
        )
        actual = migration.create_table_sql(self.old_table)
        assert expected == actual

    @pytest.yield_fixture
    def mock_create_table(self):
        with mock.patch.object(
            RedshiftSchemaMigration,
            'create_table_sql',
            return_value='create',
            autospect=True
        ) as mock_create_table:
            yield mock_create_table

    @pytest.yield_fixture
    def mock_insert_table(self):
        with mock.patch.object(
            RedshiftSchemaMigration,
            'insert_table_sql',
            return_value='insert',
            autospect=True
        ) as mock_insert_permission:
            yield mock_insert_permission

    @pytest.yield_fixture
    def mock_rename_table(self):
        with mock.patch.object(
            RedshiftSchemaMigration,
            'rename_table_sql',
            return_value='rename',
            autospect=True
        ) as mock_rename_table:
            yield mock_rename_table

    @pytest.yield_fixture
    def mock_drop_table(self):
        with mock.patch.object(
            RedshiftSchemaMigration,
            'drop_table_sql',
            return_value='drop',
            autospect=True
        ) as mock_drop_table:
            yield mock_drop_table

    @pytest.yield_fixture
    def mock_grant_permission(self):
        with mock.patch.object(
            RedshiftSchemaMigration,
            'grant_permission_sql',
            return_value='grant',
            autospect=True
        ) as mock_grant_permission:
            yield mock_grant_permission

    def test_create_simple_push_plan_with_new_table(
        self,
        migration,
        mock_create_table,
        mock_grant_permission
    ):
        expected = ['BEGIN;', 'create', '', 'grant', 'grant', 'COMMIT;']
        actual = migration.create_simple_push_plan(self.new_table)

        assert expected == actual
        assert 1 == mock_create_table.call_count
        mock_grant_permission.assert_has_calls(
            [mock.call(permission) for permission in self.permissions]
        )

    def test_create_simple_push_plan_with_different_name(
        self,
        migration,
        mock_create_table,
        mock_insert_table,
        mock_grant_permission
    ):
        expected = ['BEGIN;', 'create', 'insert', 'grant', 'grant', 'COMMIT;']
        actual = migration.create_simple_push_plan(
            self.new_table,
            self.another_old_table
        )

        assert expected == actual
        assert 1 == mock_create_table.call_count
        assert 1 == mock_insert_table.call_count
        mock_grant_permission.assert_has_calls(
            [mock.call(permission) for permission in self.permissions]
        )

    def test_create_simple_push_plan_with_existing_table(
        self,
        migration,
        mock_create_table,
        mock_insert_table,
        mock_rename_table,
        mock_grant_permission,
        mock_drop_table
    ):
        expected = ['BEGIN;', 'create', 'insert', 'rename', 'rename',
                    'grant', 'grant', 'drop', 'COMMIT;']
        actual = migration.create_simple_push_plan(
            self.new_table,
            self.old_table
        )

        assert expected == actual
        assert 1 == mock_create_table.call_count
        assert 1 == mock_insert_table.call_count
        mock_rename_table.assert_has_calls([
            mock.call(self.old_table.name, self.old_table.name + '_old'),
            mock.call(self.new_table.name + '_tmp', self.new_table.name)
        ])
        mock_grant_permission.assert_has_calls(
            [mock.call(permission) for permission in self.permissions]
        )
        mock_drop_table.called_once_with(self.old_table.name + '_old')
