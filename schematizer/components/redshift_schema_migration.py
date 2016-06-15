# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

from schematizer.models import redshift_data_types as data_types
from schematizer.models.sql_entities import MetaDataKey


class RedshiftSchemaMigration(object):
    """
    Generate push plans that update Redshift schemas. The push plans
    are not actually ran.

    TODO[clin|DATAPIPE-160]: check schema compatibility
    """

    def create_simple_push_plan(self, new_table, old_table=None):
        """The generated Redshift table migration push plan will create a new
        table, and when there is an old table, copy data from the old table,
        and drop the old table.

        If the new table name is different from the old table name, the new
        table is created but the old table remains after the data is copied.

        :param new_table: SQLTable object that represents a new Redshift table
        :return List of Redshift SQL commands
        :param old_table: SQLTable object that represents an existing Redshift
        table. None if the table does not exist.

        Currently the dropping table is left out from the push plan. It is
        tracked in DATAPIPE-174.
        TODO[clin|DATAPIPE-174]: handle dropping old table
        """
        update_existing_table = (old_table and
                                 old_table.name == new_table.name)
        return (self.get_update_existing_table_push_plan(old_table, new_table)
                if update_existing_table
                else self.get_create_new_table_push_plan(new_table, old_table))

    def get_create_new_table_push_plan(self, new_table, old_table=None):
        """Push plan that either creates the new table when old table does
        not exist, or create the new table and copy data from the old table
        when new table has a different name from the old table.
        """
        permissions = self.get_permissions(new_table)

        plan = list()
        plan.append(self.begin_transaction_sql())

        if new_table.schema_name:
            plan.append(self.create_schema_sql(new_table))

        plan.append(self.create_table_sql(new_table))
        plan.append(self.insert_table_sql(new_table, old_table))
        plan.extend(self.grant_permission_sqls(permissions))
        plan.append(self.commit_cmd_sql())
        return plan

    def get_update_existing_table_push_plan(self, old_table, new_table):
        permissions = self.get_permissions(new_table)
        # cloning the object is mainly for code readability; if performance
        # is affected, temporarily changing new_table.name and reverting it
        # back will work as well.
        tmp_table = copy.deepcopy(new_table)
        tmp_table.name += '_tmp'
        drop_table_name = old_table.name + '_old'

        plan = list()
        plan.append(self.begin_transaction_sql())

        if tmp_table.schema_name:
            plan.append(self.create_schema_sql(tmp_table))

        plan.append(self.create_table_sql(tmp_table))
        plan.append(self.insert_table_sql(tmp_table, old_table))
        plan.append(self.rename_table_sql(
            old_table.full_name,
            drop_table_name
        ))
        plan.append(self.rename_table_sql(tmp_table.full_name, new_table.name))
        plan.extend(self.grant_permission_sqls(permissions))
        plan.append(self.drop_table_sql(drop_table_name))
        plan.append(self.commit_cmd_sql())
        return plan

    @classmethod
    def create_schema_sql(cls, table):
        return 'CREATE SCHEMA IF NOT EXISTS {schema}'.format(
            schema=table.schema_name
        )

    @classmethod
    def create_table_sql(cls, table):
        begin_create_table = 'CREATE TABLE {table} ('.format(
            table=table.full_name
        )
        end_create_table = ');'

        definitions = [cls.get_column_def_sql(col) for col in table.columns]
        primary_key_sql = cls.get_primary_key_sql(table)
        if primary_key_sql:
            definitions.append(primary_key_sql)

        return ''.join(
            (begin_create_table, ','.join(definitions), end_create_table)
        )

    @classmethod
    def get_column_def_sql(cls, column):
        nullable = '' if column.is_nullable else ' not null'
        default = ''
        if column.default_value is not None:
            default = ' default {0}'.format(
                cls.convert_none_or_string(column.default_value)
            )
        constraints = cls.concatenate_attributes(column.attributes)
        return '{name} {data_type}{nullable}{default}{attributes}'.format(
            name=column.name,
            data_type=cls.construct_data_type(column.type),
            nullable=nullable,
            default=default,
            attributes=' ' + constraints if constraints else ''
        )

    @classmethod
    def construct_data_type(cls, column_type):
        if isinstance(column_type, data_types.RedshiftString):
            return '{0}({1})'.format(column_type.type_name, column_type.length)
        if isinstance(column_type, data_types.RedshiftRealNumber):
            return '{0}({1},{2})'.format(
                column_type.type_name,
                column_type.precision,
                column_type.scale
            )
        return column_type.type_name

    @classmethod
    def concatenate_attributes(cls, attributes, delimiter=' '):
        str_list = (('{name} {value}' if attr.has_value else '{name}').format(
            name=attr.name,
            value=cls.convert_none_or_string(attr.value)
        ) for attr in attributes)
        return delimiter.join(str_list)

    @classmethod
    def convert_none_or_string(cls, value):
        if value is None:
            return 'null'
        if isinstance(value, basestring):
            new_value = value.replace('\'', '\\\'')
            return '\'{0}\''.format(new_value)
        return value

    @classmethod
    def get_primary_key_sql(cls, table):
        primary_key_cols = sorted(
            (c for c in table.columns if c.primary_key_order),
            key=lambda c: c.primary_key_order
        )
        primary_keys = ', '.join(col.name for col in primary_key_cols)
        return ('PRIMARY KEY ({0})'.format(primary_keys)
                if primary_keys else '')

    @classmethod
    def insert_table_sql(cls, new_table, src_table=None):
        if not src_table:
            return ''

        # Only copy data from the columns that exist in both tables.
        new_column_names = set(col.name for col in new_table.columns)
        alias_to_column_map = dict(
            (col.metadata.get(MetaDataKey.ALIASES), col)
            for col in new_table.columns
            if col.metadata.get(MetaDataKey.ALIASES)
        )
        col_pairs = []
        for src_column in src_table.columns:
            if src_column.name in new_column_names:
                col_pairs.append((src_column.name, src_column.name))
                continue

            new_column = alias_to_column_map.get(src_column.name)
            if new_column:
                col_pairs.append((src_column.name, new_column.name))

        return ('INSERT INTO {new_table} ({new_columns}) '
                '(SELECT {src_columns} FROM {src_table});'
                .format(
                    new_table=new_table.full_name,
                    new_columns=', '.join(new_col for _, new_col in col_pairs),
                    src_columns=', '.join(src_col for src_col, _ in col_pairs),
                    src_table=src_table.full_name))

    @classmethod
    def rename_table_sql(cls, old_table_full_name, new_table_name):
        return 'ALTER TABLE {old_table} RENAME TO {new_table};'.format(
            old_table=old_table_full_name,
            new_table=new_table_name
        )

    @classmethod
    def get_permissions(cls, table):
        return table.metadata.get(MetaDataKey.PERMISSION) or []

    @classmethod
    def grant_permission_sqls(cls, permissions):
        return [cls.grant_permission_sql(permission)
                for permission in permissions or []]

    @classmethod
    def grant_permission_sql(cls, permission):
        return 'GRANT {permission} ON {object} TO {target};'.format(
            permission=permission.permission,
            object=permission.object_name,
            target=('GROUP {0}'.format(permission.user_or_group_name)
                    if permission.for_group
                    else permission.user_or_group_name)
        )

    @classmethod
    def drop_table_sql(cls, table_name):
        return 'DROP TABLE {table};'.format(table=table_name)

    @classmethod
    def begin_transaction_sql(cls):
        return 'BEGIN;'

    @classmethod
    def commit_cmd_sql(cls):
        return 'COMMIT;'
