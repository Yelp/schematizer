# -*- coding: utf-8 -*-
import unittest
from cached_property import cached_property
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPServerError

from schematizer.views.namespaces import list_namespaces
from schematizer.views.namespaces import list_sources_by_namespace
from tests.models.domain_test import DomainFactory


class TestListSourcesByNamespace(unittest.TestCase):

    @cached_property
    def default_sources(self):
        return [DomainFactory.create_domain_object()]

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'namespace': 'yelp'}
        return dummy_request

    @patch(
        'schematizer.models.domain.list_sources_by_namespace',
        return_value=[]
    )
    def test_none_existing_namespace(
            self,
            list_sources_by_namespace_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            list_sources_by_namespace,
            self.request_mock
        )

    @patch(
        'schematizer.models.domain.list_sources_by_namespace',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            list_sources_by_namespace_mock
    ):
        self.assertRaises(
            HTTPServerError,
            list_sources_by_namespace,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.domain.list_sources_by_namespace',
            return_value=self.default_sources
        ):
            sources = list_sources_by_namespace(
                self.request_mock
            )
            self.assertEqual(
                sources,
                [source.to_dict() for source in self.default_sources]
            )


class TestListNamespaces(unittest.TestCase):

    @cached_property
    def default_namespaces(self):
        return ["yelp"]

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        return dummy_request

    @patch(
        'schematizer.models.domain.list_all_namespaces',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            list_namespaces_mock
    ):
        self.assertRaises(
            HTTPServerError,
            list_namespaces,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.domain.list_all_namespaces',
            return_value=self.default_namespaces
        ):
            namespaces = list_namespaces(
                self.request_mock
            )
            self.assertEqual(namespaces, self.default_namespaces)
