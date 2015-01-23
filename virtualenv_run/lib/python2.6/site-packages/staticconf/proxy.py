"""
Proxy a configuration value. Defers the lookup until the value is used, so that
values can be read statically at import time.
"""
import functools
import operator

from staticconf import errors
import six


class UndefToken(object):
    """A token to represent an undefined value, so that None can be used
    as a default value.
    """
    def __repr__(self):
        return "<Undefined>"

UndefToken = UndefToken()


_special_names = [
    '__abs__', '__add__', '__and__', '__bool__', '__call__', '__cmp__',
    '__coerce__',
    '__contains__', '__delitem__', '__delslice__', '__div__', '__divmod__',
    '__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__',
    '__getslice__', '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__',
    '__idiv__', '__idivmod__', '__ifloordiv__', '__ilshift__', '__imod__',
    '__imul__', '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__',
    '__isub__', '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__',
    '__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__',
    '__neg__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__',
    '__rand__', '__rdiv__', '__rdivmod__',
    '__repr__', '__reversed__', '__rfloorfiv__', '__rlshift__', '__rmod__',
    '__rmul__', '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
    '__rtruediv__', '__rxor__', '__setitem__', '__setslice__', '__sub__',
    '__truediv__', '__xor__', 'next', '__nonzero__', '__str__', '__unicode__',
    '__index__',
]


unary_funcs = {
    '__unicode__':  six.text_type,
    '__str__':      str,
    '__repr__':     repr,
    '__nonzero__':  bool,  # Python2 bool
    '__bool__':     bool,  # Python3 bool
    '__hash__':     hash,
}


def build_class_def(cls):
    def build_method(name):
        def method(self, *args, **kwargs):
            if name in unary_funcs:
                return unary_funcs[name](self.value)

            if hasattr(operator, name):
                return getattr(operator, name)(self.value, *args)

            return getattr(self.value, name)(*args, **kwargs)
        return method

    namespace = dict((name, build_method(name)) for name in _special_names)
    return type(cls.__name__, (cls,), namespace)


def cache_as_field(cache_name):
    """Cache a functions return value as the field 'cache_name'."""
    def cache_wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(self, *args, **kwargs):
            value = getattr(self, cache_name, UndefToken)
            if value != UndefToken:
                return value

            ret = func(self, *args, **kwargs)
            setattr(self, cache_name, ret)
            return ret
        return inner_wrapper
    return cache_wrapper


def extract_value(proxy):
    """Given a value proxy type, Retrieve a value from a namespace, raising
    exception if no value is found, or the value does not validate.
    """
    value = proxy.namespace.get(proxy.config_key, proxy.default)
    if value is UndefToken:
        raise errors.ConfigurationError("%s is missing value for: %s" %
            (proxy.namespace, proxy.config_key))

    try:
        return proxy.validator(value)
    except errors.ValidationError as e:
        raise errors.ConfigurationError("%s failed to validate %s: %s" %
            (proxy.namespace, proxy.config_key, e))


class ValueProxy(object):
    """Proxy a configuration value so it can be loaded after import time."""
    __slots__ = [
        'validator',
        'config_key',
        'default',
        '_value',
        'namespace',
        '__weakref__'
    ]

    @classmethod
    @cache_as_field('_class_def')
    def get_class_def(cls):
        return build_class_def(cls)

    def __new__(cls, *args, **kwargs):
        """Create instances of this class with proxied special names."""
        klass = cls.get_class_def()
        instance = object.__new__(klass)
        klass.__init__(instance, *args, **kwargs)
        return instance

    def __init__(self, validator, namespace, key, default=UndefToken):
        self.validator      = validator
        self.config_key     = key
        self.default        = default
        self.namespace      = namespace
        self._value         = UndefToken

    @cache_as_field('_value')
    def get_value(self):
        return extract_value(self)

    value = property(get_value)

    def __getattr__(self, item):
        return getattr(self.value, item)

    def reset(self):
        """Clear the cached value so that configuration can be reloaded."""
        self._value = UndefToken
