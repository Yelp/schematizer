# -*- coding: utf-8 -*-
import abc


class AvroConvertible(object):
    """
    Implement this abstract class to enable the ability to convert
    source schema to Avro schema.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_avro_schema(self, src_schema):
        """
        Create Avro schema from the given source schema.
        :param src_schema: source schema object.
        :return: json object that represents Avro schema of given source
        schema. It returns None if `src_schema` is None.
        """
        raise NotImplementedError()


class RedshiftConvertible(object):
    """
    Implement this abstract class to enable the ability to convert
    source schema to Redshift table schema.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_redshift_schema(self, src_schema):
        """
        Create Redshift table schema from the given source schema.
        :param src_schema: source schema object.
        :return: SQLTable object that represents Redshift table schema of given
        source schema. It returns None if `src_schema` is None.
        """
        raise NotImplementedError()


class SchemaConversionException(Exception):
    pass


class UnsupportedTypeException(SchemaConversionException):
    pass


class MetaDataKeyEnum(object):
    """Valid metadata keys that can be added in the Avro Field type"""

    FIX_LEN = 'fixlen'     # length of char type
    MAX_LEN = 'maxlen'     # length of varchar type
    PRIMARY_KEY = 'pkey'   # whether it is primary key
    TIMESTAMP = 'ts'       # whether it is a timestamp field
    UNSIGNED = 'unsigned'  # whether the int type is unsigned
    LENGTH = 'length'      # precision of decimal/numeric type
    DECIMAL = 'decimal'    # scale of decimal/numeric type
