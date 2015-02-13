# -*- coding: utf-8 -*-
from schema_resolution import SchemaCompatibilityValidator


def is_backward_compatible(old_schema, new_schema):
    """Whether the data serialized using specified old_schema can be
    deserialized using specified new_schema.
    """
    return SchemaCompatibilityValidator.is_backward_compatible(
        old_schema,
        new_schema
    )


def is_forward_compatible(old_schema, new_schema):
    """Whether the data serialized using specified new_schema can be
    deserialized using specified old_schema.
    """
    return SchemaCompatibilityValidator.is_backward_compatible(
        new_schema,
        old_schema
    )


def is_full_compatible(old_schema, new_schema):
    """Whether the data serialized using specified old_schema can be
    deserialized using specified new_schema, and vice versa.
    """
    return (SchemaCompatibilityValidator.is_backward_compatible(
        new_schema,
        old_schema
    ) and SchemaCompatibilityValidator.is_backward_compatible(
        old_schema,
        new_schema
    ))


class InvalidRegisterOperation(Exception):
    pass


class SchemaRepository(object):
    """
    Repository that contains a set of functions to work with Avro schemas.
    """

    def add_schema_from_sql(self, sqls):
        """Add an Avro schema generated from given sql statement(s) into
        the schema store.

        :param sqls: a sequence of mysql table change sql statements.
        :return: It returns created AvroSchema object.
        """
        # Create an Avro schema from the sql statements
        # Generate the corresponding topic name
        # If the new schema is compatible with existing schemas of the topic,
        # add the schema to the schema store. Otherwise, create a new topic,
        # and then insert the schema.
        pass

    def generate_topic_name(self, avro_schema):
        """Generate the topic name of given Avro schema.
        Note that this is meant to be used internally and not be exposed to
        external caller.
        """
        # The namespace of an schema can be composed from the domain info.
        # namespace = namespace.source
        # topic name = (schema fullname).(version_number(id?))
        pass

    def create_topic(self, topic_name, namespace, source):
        """Create a topic named `topic_name` in the domain of given
        namespace and source. It returns the ID of the newly created topic.
        """
        pass

    def create_domain(self, namespace, source, owner_email):
        """Create a domain of specified namespace and source.
        It returns the ID of newly created domain.
        """
        pass

    def get_domain_by_id(self, domain_id):
        pass

    def search_domains(self, namespace=None, source=None):
        """Get a list of domains that match the specified namespace and/or
        source if they're provided.
        """

    def get_topic_by_id(self, topic_id):
        pass

    def get_latest_topic(self, namespace, source):
        """Get latest topic of given namespace and source.
        The latest one is based on the `created_at` timestamp of topics.
        """
        pass

    def get_latest_topic_by_domain_id(self, domain_id):
        pass

    def get_schema_by_id(self, schema_id):
        """Get Avro schema of given schema id along with its metadata."""
        pass

    def get_latest_schema_of_topic(self, topic):
        """Get latest enabled (RW or R) schema of given topic"""
        pass

    def get_schemas_by_topic(self, topic, include_disabled=False):
        """Get all the Avro schemas of specified topic."""
        pass

    def validate_schema(self, target_schema):
        """Check whether given schema string is a valid Avro schema, and
        compatible with existing schemas of same topic.

        :param target_schema: Avro schema string to be verified
        :return: True/False to indicate if given schema is valid and compatible
        """
        # Get topic of specified schema
        # Check the compatibility of target schema against all the non-disabled
        # schemas of this topic. Return False as soon as an incompatible one is
        # found.
        pass

    def mark_schema_disabled(self, schema_id):
        """Disable the Avro schema of specified schema id."""
        pass

    def mark_schema_readonly(self, schema_id):
        """Mark the Avro schema of specified schema id as read-only."""
        pass

    def register_producer(self, producer_email, schema_id, job_name,
                          expected_freq):
        """Register a producer to the specified Avro schema.

        :param producer_email: single email of the producer
        :param schema_id: Id of the Avro schema the producer wants to register
        :param job_name: the service/batch job where this producer lives
        :param expected_freq: how often this producer will be using this schema
        (estimated seconds)
        """
        pass

    def register_consumer(self, consumer_email, schema_id, job_name,
                          expected_freq):
        """Register a consumer to the specified Avro schema.

        :param consumer_email: single email of the consumer
        :param schema_id: Id of the Avro schema the consumer wants to register
        :param job_name: the service/batch job where this consumer lives
        :param expected_freq: how often this consumer will be using this schema
        (estimated seconds)
        """
        pass

    def search_producers(self, schema_id=None, job_name=None):
        """Return a list of producers that register to the specified schema
        (if `schema_id` provided) and live in the given `job_name` (if
        provided).

        :param schema_id: Id of the Avro schema the producer registers to;
        it's optional and used to filter producers only when it's provided.
        :param job_name: the service/batch job where the producer lives;
        it's optional and used to filter producers only when it's provided.
        :return: A list of matched producers.
        """
        pass

    def search_consumers(self, schema_id=None, job_name=None):
        """Return a list of consumers that register to the specified schema
        (if `schema_id` provided) and live in the given `job_name` (if
        provided).

        :param schema_id: Id of the Avro schema the consumer registers to;
        it's optional and used to filter consumers only when it's provided.
        :param job_name: the service/batch job where the consumer lives;
        it's optional and used to filter consumers only when it's provided.
        :return: A list of matched consumers.
        """
        pass
