# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import staticconf.testing

from schematizer import models
from schematizer_testing import factories


@pytest.fixture
def yelp_namespace_name():
    return 'yelp'


@pytest.fixture
def yelp_namespace(yelp_namespace_name):
    return factories.create_namespace(yelp_namespace_name)


@pytest.fixture
def biz_src_name():
    return 'biz'


@pytest.fixture
def another_biz_src_name():
    return 'biz_v2'


@pytest.fixture
def biz_source(yelp_namespace, biz_src_name):
    return factories.create_source(
        namespace_name=yelp_namespace.name,
        source_name=biz_src_name,
        owner_email='test-src@yelp.com'
    )


@pytest.fixture
def another_biz_source(yelp_namespace, another_biz_src_name):
    return factories.create_source(
        namespace_name=yelp_namespace.name,
        source_name=another_biz_src_name,
        owner_email='test-src@yelp.com'
    )


@pytest.fixture
def biz_topic(biz_source):
    return factories.create_topic(
        topic_name='yelp.biz.1',
        namespace_name=biz_source.namespace.name,
        source_name=biz_source.name
    )


@pytest.fixture
def biz_schema_json():
    return {
        "name": "biz",
        "type": "record",
        "fields": [{"name": "id", "type": "int", "doc": "id", "default": 0}],
        "doc": "biz table"
    }


@pytest.fixture
def biz_schema_elements():
    return [
        models.AvroSchemaElement(
            key='biz',
            element_type='record',
            doc="biz table"
        ),
        models.AvroSchemaElement(
            key='biz|id',
            element_type='field',
            doc="id"
        )
    ]


@pytest.fixture
def biz_schema(biz_topic, biz_schema_json, biz_schema_elements):
    return factories.create_avro_schema(
        biz_schema_json,
        biz_schema_elements,
        topic_name=biz_topic.name,
        namespace=biz_topic.source.namespace.name,
        source=biz_topic.source.name
    )


@pytest.fixture
def disabled_schema_json():
    return {
        "type": "record",
        "name": "disabled",
        "fields": [{"name": "id", "type": "int", "doc": "id"}],
        "doc": "I am disabled!"
    }


@pytest.fixture
def disabled_schema_elements():
    return [
        models.AvroSchemaElement(
            key='disabled',
            element_type='record',
            doc="I am disabled!"
        ),
        models.AvroSchemaElement(
            key='disabled|id',
            element_type='field',
            doc="id"
        )
    ]


@pytest.fixture
def disabled_schema(biz_topic, disabled_schema_json, disabled_schema_elements):
    return factories.create_avro_schema(
        disabled_schema_json,
        disabled_schema_elements,
        topic_name=biz_topic.name,
        status=models.AvroSchemaStatus.DISABLED
    )


@pytest.fixture
def biz_pkey_topic(biz_source):
    return factories.create_topic(
        topic_name='yelp.biz.2',
        namespace_name=biz_source.namespace.name,
        source_name=biz_source.name
    )


@pytest.fixture
def biz_pkey_schema_json():
    return {
        "name": "biz_pkey",
        "type": "record",
        "fields": [
            {"name": "id_1", "type": "int", "doc": "id1", "pkey": 1},
            {"name": "f_1", "type": "int", "doc": "f_1"},
            {"name": "id_2", "type": "int", "doc": "id2", "pkey": 2},
        ],
        "doc": "biz table with pkey"
    }


@pytest.fixture
def biz_pkey_schema_elements():
    return [
        models.AvroSchemaElement(
            key='biz_pkey',
            element_type='record',
            doc="biz table"
        ),
        models.AvroSchemaElement(
            key='biz_pkey|id_1',
            element_type='field',
            doc="id"
        )
    ]


@pytest.fixture
def biz_pkey_schema(
    biz_pkey_topic,
    biz_pkey_schema_json,
    biz_pkey_schema_elements
):
    return factories.create_avro_schema(
        biz_pkey_schema_json,
        biz_pkey_schema_elements,
        topic_name=biz_pkey_topic.name,
        namespace=biz_pkey_topic.source.namespace.name,
        source=biz_pkey_topic.source.name
    )


@pytest.fixture
def biz_src_refresh(biz_source):
    return factories.create_refresh(
        source_id=biz_source.id,
        offset=0,
        batch_size=500,
        priority=models.Priority.MEDIUM.name,
        filter_condition=None,
        avg_rows_per_second_cap=1000
    )


@pytest.fixture
def dw_data_target():
    return factories.create_data_target(
        target_type='dw_redshift',
        destination='dwv1.redshift.yelpcorp.com'
    )


@pytest.fixture
def dw_consumer_group(dw_data_target):
    return factories.create_consumer_group('dw', dw_data_target)


@pytest.fixture
def dw_consumer_group_namespace_data_src(dw_consumer_group, yelp_namespace):
    return factories.create_consumer_group_data_source(
        dw_consumer_group,
        data_src_type=models.DataSourceTypeEnum.NAMESPACE,
        data_src_id=yelp_namespace.id
    )


@pytest.fixture
def dw_consumer_group_source_data_src(dw_consumer_group, biz_source):
    return factories.create_consumer_group_data_source(
        dw_consumer_group,
        data_src_type=models.DataSourceTypeEnum.SOURCE,
        data_src_id=biz_source.id
    )


@pytest.yield_fixture(autouse=True, scope='session')
def mock_namespace_whitelist():
    with staticconf.testing.MockConfiguration(
        {
            'namespace_no_doc_required': [
                'yelp_wl'
            ]
        }
    ):
        yield


@pytest.fixture
def meta_attr_namespace():
    return factories.create_namespace('yelp_meta')


@pytest.fixture
def meta_attr_source(meta_attr_namespace):
    return factories.create_source(
        namespace_name=meta_attr_namespace.name,
        source_name='dummy_meta_attr',
        owner_email='test-meta-src@yelp.com'
    )


@pytest.fixture
def meta_attr_topic(meta_attr_source):
    return factories.create_topic(
        topic_name='yelp_meta.dummy_meta_attr.1',
        namespace_name=meta_attr_source.namespace.name,
        source_name=meta_attr_source.name
    )


@pytest.fixture
def meta_attr_schema_json():
    return {
        "name": "dummy_meta_attr",
        "type": "record",
        "fields": [
            {"name": "id", "type": "int", "doc": "id", "default": 0},
            {"name": "info", "type": "string", "doc": "dummy meta_attr info"}
        ],
        "doc": "Dummy Meta Attribute"
    }


@pytest.fixture
def meta_attr_schema_elements():
    return [
        models.AvroSchemaElement(
            key='dummy_meta_attr',
            element_type='record',
            doc="Meta Attr table"
        ),
        models.AvroSchemaElement(
            key='dummy_meta_attr|id',
            element_type='field',
            doc="id"
        )
    ]


@pytest.fixture
def meta_attr_schema(
    meta_attr_topic,
    meta_attr_schema_json,
    meta_attr_schema_elements
):
    return factories.create_avro_schema(
        meta_attr_schema_json,
        meta_attr_schema_elements,
        topic_name=meta_attr_topic.name,
        namespace=meta_attr_topic.source.namespace.name,
        source=meta_attr_topic.source.name
    )
