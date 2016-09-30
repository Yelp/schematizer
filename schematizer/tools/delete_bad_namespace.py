# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
from collections import namedtuple

from sqlalchemy.orm import exc as orm_exc

from schematizer import models
from schematizer.helpers.config_util import load_default_config
from schematizer.logic.doc_tool import get_notes_by_schemas_and_elements
from schematizer.logic.doc_tool import get_source_categories_by_criteria
from schematizer.logic.schema_repository import get_namespace_by_name
from schematizer.logic.schema_repository import get_refreshes_by_criteria
from schematizer.logic.schema_repository import get_schemas_by_criteria
from schematizer.models.database import session

ModelIdsPair = namedtuple('ModelIdsPair', ['model', 'ids'])


def parse_args():
    parser = argparse.ArgumentParser(
        description='Deletes a namespace or source and all of its '
        'associated entities (sources, topics, schemas, '
        'notes, schema_elements, refreshes, source_categories)'
    )

    parser.add_argument(
        'namespace_name',
        type=str,
        help="name of namespace to delete"
    )

    parser.add_argument(
        '-f', '--force',
        dest="force",
        action="store_true",
        default=False,
        required=False,
        help="Force deletion (ignore confirmation check)"
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


def confirm_deletion(namespace_name, source_name=None):
    print "Please retype the namespace name to confirm you want to delete it"
    confirmation = raw_input().strip()
    if confirmation != namespace_name.strip():
        raise ValueError("Given name does not match namespace name given")
    if source_name:
        print "Please retype the source name to confirm you want to delete it"
        confirmation = raw_input().strip()
        if confirmation != source_name.strip():
            raise ValueError("Given name does not match source name given")


def delete_all(namespace_name, source_name=None, dry_run=False):
    with session.connect_begin(ro=False):
        namespace = get_namespace_by_name(namespace_name)
        delete_all_children(
            get_all_children(
                namespace_name,
                source_name=source_name
            ),
            dry_run=dry_run
        )
        if not dry_run and not source_name:
            session.delete(namespace)


def delete_all_children(children, dry_run=False):
    for model_id_pair in children:
        model = model_id_pair.model
        model_name = model.__name__
        ids = model_id_pair.ids
        if ids:
            new_query = session.query(
                model
            ).filter(model.id.in_(ids))

            if dry_run:
                all_items = new_query.all()
                print "Objects for {} ({} found): {}".format(
                    model_name, len(all_items), all_items
                )
            else:
                print "Deleting {} items of type {}".format(
                    len(ids), model_name
                )
                new_query.delete(synchronize_session=False)
        else:
            print "No items found for {}. Not deleting anything".format(
                model_name
            )


def get_all_children(namespace_name, source_name=None):
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
        ModelIdsPair(models.Note, note_ids),
        ModelIdsPair(models.AvroSchemaElement, element_ids),
        ModelIdsPair(models.AvroSchema, schema_ids),
        ModelIdsPair(models.SourceCategory, category_ids),
        ModelIdsPair(models.Topic, topic_ids),
        ModelIdsPair(models.Refresh, refresh_ids),
        ModelIdsPair(models.Source, source_ids)
    ]


def run():
    args = parse_args()
    namespace_name = args.namespace_name
    source_name = args.source_name
    dry_run = args.dry_run
    load_default_config("config.yaml")
    if not args.force:
        confirm_deletion(namespace_name, source_name=source_name)
    try:
        delete_all(
            namespace_name=namespace_name,
            source_name=source_name,
            dry_run=dry_run
        )
    except orm_exc.NoResultFound:
        print "No namespace found with name: {}".format(namespace_name)


if __name__ == '__main__':
    run()
