"""
Validate a configuration value by converting it to a specific type.

These functions are used by :mod:`staticconf.readers` and
:mod:`staticconf.schema` to coerce config values to a type.
"""
import datetime
import logging
import re
import time

import six

from staticconf.errors import ValidationError


def validate_string(value):
    return None if value is None else six.text_type(value)


def validate_bool(value):
    return None if value is None else bool(value)


def validate_numeric(type_func, value):
    try:
        return type_func(value)
    except ValueError:
        raise ValidationError("Invalid %s: %s" % (type_func.__name__, value))


def validate_int(value):
    return validate_numeric(int, value)


def validate_float(value):
    return validate_numeric(float, value)


date_formats = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %I:%M:%S %p",
    "%Y-%m-%d",
    "%y-%m-%d",
    "%m/%d/%y",
    "%m/%d/%Y",
]


def validate_datetime(value):
    if isinstance(value, datetime.datetime):
        return value

    for format_ in date_formats:
        try:
            return datetime.datetime.strptime(value, format_)
        except ValueError:
            pass
    raise ValidationError("Invalid date format: %s" % value)


def validate_date(value):
    if isinstance(value, datetime.date):
        return value

    return validate_datetime(value).date()


time_formats = [
    "%I %p",
    "%H:%M",
    "%I:%M %p",
    "%H:%M:%S",
    "%I:%M:%S %p"
]


def validate_time(value):
    if isinstance(value, datetime.time):
        return value

    for format_ in time_formats:
        try:
            return datetime.time(*time.strptime(value, format_)[3:6])
        except ValueError:
            pass
    raise ValidationError("Invalid time format: %s" % value)


def _validate_iterable(iterable_type, value):
    """Convert the iterable to iterable_type, or raise a Configuration
    exception.
    """
    if isinstance(value, six.string_types):
        msg = "Invalid iterable of type(%s): %s"
        raise ValidationError(msg % (tuple(value), value))

    try:
        return iterable_type(value)
    except TypeError:
        raise ValidationError("Invalid iterable: %s" % (value))


def validate_list(value):
    return _validate_iterable(list, value)


def validate_set(value):
    return _validate_iterable(set, value)


def validate_tuple(value):
    return _validate_iterable(tuple, value)


def validate_regex(value):
    try:
        return re.compile(value)
    except (re.error, TypeError) as e:
        raise ValidationError("Invalid regex: %s, %s" % (e, value))


def build_list_type_validator(item_validator):
    """Return a function which validates that the value is a list of items
    which are validated using item_validator.
    """
    def validate_list_of_type(value):
        return [item_validator(item) for item in validate_list(value)]
    return validate_list_of_type


def build_map_type_validator(item_validator):
    """Return a function which validates that the value is a mapping of
    items. The function should return pairs of items that will be
    passed to the `dict` constructor.
    """
    def validate_mapping(value):
        return dict(item_validator(item) for item in validate_list(value))
    return validate_mapping


def validate_log_level(value):
    """Validate a log level from a string value. Returns a constant from
    the :mod:`logging` module.
    """
    try:
        return getattr(logging, value)
    except AttributeError:
        raise ValidationError("Unknown log level: %s" % value)


def validate_any(value):
    return value


validators = {
    '':          validate_any,
    'bool':      validate_bool,
    'date':      validate_date,
    'datetime':  validate_datetime,
    'float':     validate_float,
    'int':       validate_int,
    'list':      validate_list,
    'set':       validate_set,
    'string':    validate_string,
    'time':      validate_time,
    'tuple':     validate_tuple,
    'regex':     validate_regex,
    'log_level': validate_log_level,
}


def get_validators():
    """Return an iterator of (validator_name, validator) pairs."""
    return six.iteritems(validators)
