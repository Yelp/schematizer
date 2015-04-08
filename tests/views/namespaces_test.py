# -*- coding: utf-8 -*-
import pytest
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound

from schematizer.utils.decorators import
from schematizer.views import constants
from schematizer.views.namespaces import list_namespaces
from schematizer.views.namespaces import list_sources_by_namespace
from testing import factories


class TestListSourcesByNamespace(object):

    @property
    def default_sources(self):
        return [
            factories.DomainFactory.create(
                factories.fake_namespace,
                factories.fake_source
            )
        ]

    @property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'namespace': 'yelp'}
        return dummy_request

    @patch(
        'schematizer.logic.schema_repository.get_domains_by_namespace',
        return_value=[]
    )
    def test_none_existing_namespace(
        self,
        get_domains_by_namespace_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            list_sources_by_namespace(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self):
        with patch(
            'schematizer.logic.schema_repository.get_domains_by_namespace',
            return_value=self.default_sources
        ):
            sources = list_sources_by_namespace(
                self.request_mock
            )
            assert sources == [
                source.to_dict() for source in self.default_sources
            ]


class TestListNamespaces(object):

    @property
    def default_namespaces(self):
        return [factories.fake_namespace]

    @property
    def request_mock(self):
        dummy_request = Mock()
        return dummy_request

    def test_no_namespaces(self):
        with patch(
            'schematizer.logic.schema_repository.get_namespaces',
            return_value=[]
        ):
            namespaces = list_namespaces(
                self.request_mock
            )
            assert namespaces == []

    def test_happy_case(self):
        with patch(
            'schematizer.logic.schema_repository.get_namespaces',
            return_value=self.default_namespaces
        ):
            namespaces = list_namespaces(
                self.request_mock
            )
            assert namespaces == self.default_namespaces
