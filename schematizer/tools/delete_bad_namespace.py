# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse

from sqlalchemy.orm import exc as orm_exc
from yelp_servlib.config_util import load_default_config

from schematizer import models
from schematizer.logic.doc_tool import get_notes_by_schemas_and_elements
from schematizer.logic.doc_tool import get_source_categories_by_criteria
from schematizer.logic.schema_repository import get_namespace_by_name
from schematizer.logic.schema_repository import get_refreshes_by_criteria
from schematizer.logic.schema_repository import get_schemas_by_criteria
from schematizer.models.database import session


def parse_args():
    parser = argparse.ArgumentParser(
        description='Deletes a namespace (or source) and all of its children '
        '(sources, topics, schemas, notes, schema_elements, '
        'refreshes, source_categories)'
    )

    parser.add_argument(
        'namespace_name',
        type=str,
        help="name of namespace to delete"
    )

    parser.add_argument(
        '--f', '--force',
        dest="force",
        action="store_true",
        default=False,
        required=False,
        help="Force deletion and ignoring active namespace check"
    )

    parser.add_argument(
        '--dry-run',
        action="store_true",
        default=False,
        required=False,
        help="Instead of deleting, this will print the names of "
             "the objects to be deleted"
    )

    parser.add_argument(
        '--source-name',
        type=str,
        default=None,
        required=False,
        help="For limiting destruction to a single source contained "
             "in the namespace. This will only delete this source and "
             "all of it's children (topics, schemas, notes, "
             "schema_elements, refreshes, source_categories)"
    )

    return parser.parse_args()


def confirm_deletion(namespace_name):
    print "Please retype the namespace name to confirm you want to delete it"
    confirmation = raw_input().strip()
    if confirmation != namespace_name.strip():
        raise ValueError("Given name does not match namespace name given")


def delete_all_children(children, dry_run):
    for model_id_pair in children:
        model = model_id_pair['model']
        model_name = model.__name__
        ids = model_id_pair['ids']
        if ids:
            new_query = session.query(
                model
            ).filter(
                model.id.in_(ids)
            )

            if dry_run:
                all_items = new_query.all()
                print "Objects for {} ({} found): {}".format(
                    model_name,
                    len(all_items),
                    all_items
                )
            else:
                print "Deleting {} items of type {}".format(
                    len(ids),
                    model_name
                )
                new_query.delete(synchronize_session=False)
        else:
            print "No items found for {}. Not deleting anything".format(
                model_name
            )


def _create_model_id_pair(model, ids):
    return {
        'model': model,
        'ids': ids
    }


def get_all_namespace_children(namespace_name, source_name=None):
    schemas = get_schemas_by_criteria(
        namespace_name,
        source_name=source_name
    )
    schema_ids = set(o.id for o in schemas)
    topic_ids = set(o.topic.id for o in schemas)
    source_ids = set(o.topic.source.id for o in schemas)
    elements = []
    for schema in schemas:
        elements.extend(schema.avro_schema_elements)
    element_ids = set(o.id for o in elements)
    notes = get_notes_by_schemas_and_elements(schemas, elements)
    note_ids = set(o.id for o in notes)
    categories = get_source_categories_by_criteria(
        namespace_name, source_name=source_name
    )
    category_ids = set(o.id for o in categories)
    refreshes = get_refreshes_by_criteria(
        namespace=namespace_name,
        source_name=source_name
    )
    refresh_ids = set(o.id for o in refreshes)
    return [
        _create_model_id_pair(models.Note, note_ids),
        _create_model_id_pair(models.AvroSchemaElement, element_ids),
        _create_model_id_pair(models.AvroSchema, schema_ids),
        _create_model_id_pair(models.SourceCategory, category_ids),
        _create_model_id_pair(models.Topic, topic_ids),
        _create_model_id_pair(models.Refresh, refresh_ids),
        _create_model_id_pair(models.Source, source_ids)
    ]


def run():
    args = parse_args()
    namespace_name = args.namespace_name
    load_default_config("config.yaml")
    if not args.force:
        confirm_deletion(namespace_name)
    with session.connect_begin(ro=False):
        try:
            namespace = get_namespace_by_name(namespace_name)
            delete_all_children(
                get_all_namespace_children(
                    namespace_name,
                    source_name=args.source_name
                ),
                args.dry_run
            )
            if not args.dry_run and not args.source_name:
                session.delete(namespace)
        except orm_exc.NoResultFound:
            print "No namespace found with name: {}".format(namespace_name)


if __name__ == '__main__':
    run()
