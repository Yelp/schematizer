"""
Functions to read values directly from a
:class:`staticconf.config.ConfigNamespace`.  Values will be validated and
cast to the requested type.


Examples
--------

.. code-block:: python

    import staticconf

    # read an int
    max_cycles = staticconf.read_int('max_cycles')
    start_id = staticconf.read_int('poller.init.start_id', default=0)

    # start_date will be a datetime.date
    start_date = staticconf.read_date('start_date')

    # matcher will be a regex object
    matcher = staticconf.read_regex('matcher_pattern')

    # Read a value from a different namespace
    intervals = staticconf.read_float('intervals', namespace='something')


Readers can be attached to a namespace using a :class:`NamespaceReaders`
object.

.. code-block:: python

    import staticconf

    bling_reader = staticconf.NamespaceReaders('bling')

    # These values are read from the `bling` ConfigNamespace
    currency = bling_reader.read_string('currency')
    value = bling_reader.read_float('value')


Arguments
---------

Readers accept the following kwargs:

config_key
    string configuration key using dotted notation
default
    if no `default` is given, the key must be present in the configuration.
    If the key is missing a :class:`staticconf.errors.ConfigurationError`
    is raised.
namespace
    get the value from this namespace instead of DEFAULT.


Building custom readers
-----------------------
:func:`build_reader` is a factory function which can be used for creating
custom readers from a validation function.  A validation function should handle
all exceptions and raise a :class:`staticconf.errors.ValidationError` if there
is a problem.

First create a validation function

.. code-block:: python

    def validate_currency(value):
        try:
            # Assume a tuple or a list
            name, decimal_points = value
            return Currency(name, decimal_points)
        except Exception, e:
            raise ValidationErrror(...)


Example of a custom reader:

.. code-block:: python

    from staticconf import readers

    read_currency = readers.build_reader(validate_currency)

    # Returns a Currency object using the data from the config namespace
    # at they key `currencies.usd`.
    usd_currency = read_currency('currencies.usd')


"""
from staticconf import validation, config, errors
from staticconf.proxy import UndefToken


def _read_config(config_key, config_namespace, default):
    value = config_namespace.get(config_key, default=default)
    if value is UndefToken:
        msg = '%s missing value for %s' % (config_namespace, config_key)
        raise errors.ConfigurationError(msg)
    return value


def build_reader(validator, reader_namespace=config.DEFAULT):
    """A factory method for creating a custom config reader from a validation
    function.

    :param validator: a validation function which acceptance one argument (the
                      configuration value), and returns that value casted to
                      the appropriate type.
    :param reader_namespace: the default namespace to use. Defaults to
                             `DEFAULT`.
    """
    def reader(config_key, default=UndefToken, namespace=None):
        config_namespace = config.get_namespace(namespace or reader_namespace)
        return validator(_read_config(config_key, config_namespace, default))
    return reader


class ReaderNameFactory(object):

    @staticmethod
    def get_name(name):
        return 'read_%s' % name if name else 'read'

    @staticmethod
    def get_list_of_name(name):
        return 'read_list_of_%s' % name


def get_all_accessors(name_factory):
    for name, validator in validation.get_validators():
        yield name_factory.get_name(name), validator
        yield (name_factory.get_list_of_name(name),
               validation.build_list_type_validator(validator))


class NamespaceAccessor(object):

    def __init__(self, name, accessor_map, builder):
        self.accessor_map       = accessor_map
        self.builder            = builder
        self.namespace          = name

    def __getattr__(self, item):
        if item not in self.accessor_map:
            raise AttributeError(item)
        return self.builder(self.accessor_map[item], self.namespace)

    def get_methods(self):
        return dict((name, getattr(self, name)) for name in self.accessor_map)


def build_accessor_type(name_factory, builder):
    accessor_map = dict(get_all_accessors(name_factory))
    return lambda name: NamespaceAccessor(name, accessor_map, builder)


NamespaceReaders = build_accessor_type(ReaderNameFactory, build_reader)
"""An object with all reader functions which retrieve configuration from
a named namespace, instead of `DEFAULT`.
"""


default_readers = NamespaceReaders(config.DEFAULT)
globals().update(default_readers.get_methods())

__all__ = ['NamespaceReaders'] + list(default_readers.get_methods())
