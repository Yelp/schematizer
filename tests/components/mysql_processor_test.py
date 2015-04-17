# -*- coding: utf-8 -*-
import mock
import pytest

from schematizer.components import mysql_processor


class TestMySQLProcessor(object):

    @property
    def mysql_statements(self):
        return ["stmt_1", "stmt_2"]

    @pytest.yield_fixture(autouse=True)
    def mock_sqlparser(self):
        with mock.patch('sqlparse.parse', return_value=mock.Mock()) as parser:
            yield parser

    @pytest.fixture
    def cls_foo(self):
        return self.create_mock_processor_class('p1', 3, False)

    @pytest.fixture
    def cls_bar(self):
        return self.create_mock_processor_class('p2', 2, True)

    @pytest.fixture
    def cls_baz(self):
        return self.create_mock_processor_class('p3', 1, True)

    def create_mock_processor_class(self, tag, priority, can_handle):
        mock_instance = mock.Mock(spec=mysql_processor.MySQLProcessorBase)
        mock_instance.enabled = True
        mock_instance.priority = priority
        type(mock_instance).can_handle = mock.PropertyMock(
            return_value=can_handle
        )
        mock_instance.run.return_value = mock.Mock(tag=tag)

        # mock_cls is the mock class of derived MySQLProcessor class
        mock_cls = mock.Mock(return_value=mock_instance)
        type(mock_cls).enabled = True
        type(mock_cls).priority = priority
        return mock_cls

    @pytest.yield_fixture
    def patch_processor_base_subclasses(self, cls_foo, cls_bar, cls_baz):
        with mock.patch.object(
            mysql_processor.MySQLProcessorBase,
            '__subclasses__',
            return_value=[cls_foo, cls_bar, cls_baz]
        ):
            # reloading the mysql_processors is because the patch happens
            # after the module loading in which the mysql_processors is
            # already populated
            mysql_processor.mysql_processors = mysql_processor.load_processors()
            yield

    @pytest.yield_fixture
    def patch_processor_base_with_one_subclass(self, cls_foo):
        with mock.patch.object(
            mysql_processor.MySQLProcessorBase,
            '__subclasses__',
            return_value=[cls_foo]
        ):
            mysql_processor.mysql_processors = mysql_processor.load_processors()
            yield

    @pytest.mark.usefixtures('patch_processor_base_subclasses')
    def test_process_table_schema_mysql(self):
        actual = mysql_processor.process_table_schema_mysql(
            self.mysql_statements
        )
        assert 'p2' == actual.tag

    @pytest.mark.usefixtures('patch_processor_base_with_one_subclass')
    def test_process_table_schema_mysql_without_processor(self):
        with pytest.raises(Exception):
            mysql_processor.process_table_schema_mysql(self.mysql_statements)
