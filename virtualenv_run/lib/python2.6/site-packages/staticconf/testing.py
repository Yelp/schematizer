"""
Facilitate testing of code which uses staticconf.
"""
from staticconf import config, loader


class MockConfiguration(object):
    """A a context manager which patches the configuration namespace
    while inside the context. When the context exits the old configuration
    values will be restored to that namespace.

    .. code-block:: python

        import staticconf.testing

        config = {
            ...
        }
        with staticconf.testing.MockConfiguration(config, namespace='special'):
            # Run your tests.
        ...


    :param namespace: the namespace to patch
    :param flatten: if True the configuration will be flattened (default True)
    :param args: passed directly to the constructor of :class:`dict` and used
                 as configuration data
    :param kwargs: passed directly to the constructor of :class:`dict` and used
                as configuration data
    """

    def __init__(self, *args, **kwargs):
        name                = kwargs.pop('namespace', config.DEFAULT)
        flatten             = kwargs.pop('flatten', True)
        config_data         = dict(*args, **kwargs)
        self.namespace      = config.get_namespace(name)
        self.config_data    = (dict(loader.flatten_dict(config_data)) if flatten
                              else config_data)
        self.old_values     = None

    def setup(self):
        self.old_values = dict(self.namespace.get_config_values())
        self.reset_namespace(self.config_data)
        config.reload(name=self.namespace.name)

    def teardown(self):
        self.reset_namespace(self.old_values)
        config.reload(name=self.namespace.name)

    def reset_namespace(self, new_values):
        self.namespace.configuration_values.clear()
        self.namespace.update_values(new_values)

    def __enter__(self):
        return self.setup()

    def __exit__(self, *args):
        self.teardown()
