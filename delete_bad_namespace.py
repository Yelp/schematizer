# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import argparse
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy import or_, and_

from schematizer import models
from schematizer.models.database import session
from yelp_servlib.config_util import load_default_config

def get_namespace_children_queries(namespace):
    children_queries = [
        create_notes_query(namespace.id),
        create_elements_query(namespace.id),
        create_schemas_query(namespace.id),
        create_categories_query(namespace.id),
        create_topics_query(namespace.id),
        create_refreshes_query(namespace.id),
        create_sources_query(namespace.id)
    ]
    return children_queries

def create_sources_query(namespace_id):
    return session.query(
        models.Source
    ).filter(
        models.Source.namespace_id == namespace_id
    )

def create_categories_query(namespace_id):
    return session.query(
        models.SourceCategory
    ).join(
        models.Source
    ).filter(
        models.SourceCategory.source_id == models.Source.id,
        models.Source.namespace_id == namespace_id
    )

def create_topics_query(namespace_id):
    return session.query(
        models.Topic
    ).join(
        models.Source
    ).filter(
        models.Topic.source_id == models.Source.id,
        models.Source.namespace_id == namespace_id
    )

def create_refreshes_query(namespace_id):
    return session.query(
        models.Refresh
    ).join(
        models.Source
    ).filter(
        models.Refresh.source_id == models.Source.id,
        models.Source.namespace_id == namespace_id
    )

def create_schemas_query(namespace_id):
    return session.query(
        models.AvroSchema
    ).join(
        models.Topic,
        models.Source
    ).filter(
        models.AvroSchema.topic_id == models.Topic.id,
        models.Topic.source_id == models.Source.id,
        models.Source.namespace_id == namespace_id
    )

def create_elements_query(namespace_id):
    return session.query(
        models.AvroSchemaElement
    ).join(
        models.AvroSchema,
        models.Topic,
        models.Source
    ).filter(
        models.AvroSchemaElement.avro_schema_id == models.AvroSchema.id,
        models.AvroSchema.topic_id == models.Topic.id,
        models.Topic.source_id == models.Source.id,
        models.Source.namespace_id == namespace_id
    )

def create_notes_query(namespace_id):
    # Can't use a join since sqlalchemy doesn't like the composite foreign key of Note
    schema_ids = [schema.id for schema in create_schemas_query(namespace_id).all()]
    element_ids = [element.id for element in create_elements_query(namespace_id).all()]

    return session.query(
        models.Note
    ).filter(
        or_(
            and_(
                models.Note.reference_type == models.ReferenceTypeEnum.SCHEMA,
                models.Note.reference_id.in_(schema_ids)
            ),
            and_(
                models.Note.reference_type == models.ReferenceTypeEnum.SCHEMA_ELEMENT,
                models.Note.reference_id.in_(element_ids)
            )
        )
    )

def parse_args():
    parser = argparse.ArgumentParser(
        description='Deletes a namespace and all of its children (sources, topics, schemas, notes)'
    )

    parser.add_argument(
        'namespace_name',
        help="name of namespace to delete"
    )

    parser.add_argument(
        '--f', '--force',
        dest="force",
        action="store_true",
        default=False,
        help="Force deletion and ignoring active namespace check"
    )

    return parser.parse_args()

def confirm_deletion(namespace_name):
    done = False
    while not done:
        print "Please retype the namespace name to confirm you want to delete it"
        confirmation = raw_input().strip()
        if confirmation == namespace_name.strip():
            done = True

def delete_all_children(children):
    # Can't just use .delete since we're using joins in the statements
    for query in children:
        items = query.all()
        if items:
            print "Deleting {} items".format(len(items))
            ids = [item.id for item in items]
            item_type = items[0].__class__
            new_query = session.query(
                item_type
            ).filter(
                item_type.id.in_(ids)
            )
            new_query.delete(synchronize_session=False)
        else:
            print "No items found for this type. Not deleting anything"


def run():
    args = parse_args()
    namespace_name = args.namespace_name
    load_default_config("config.yaml")
    if not args.force:
         confirm_deletion(namespace_name)
    with session.connect_begin(ro=False):
        try:
            namespace = session.query(
                models.Namespace
            ).filter(
                models.Namespace.name == namespace_name
            ).one()
            delete_all_children(get_namespace_children_queries(namespace))
            session.delete(namespace)
        except orm_exc.NoResultFound:
            print "No namespace found with name: {}".format(namespace_name)

if __name__ == '__main__':
    run()
