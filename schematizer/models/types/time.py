# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This code is originally copied from yelp-main.
https://opengrok.yelpcorp.com/xref/yelp-main/yelp/models/types/time.py
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import sys
import time

import sqlalchemy
from six import integer_types


def build_time_column(
        raw_column=False,
        default_now_value=sqlalchemy.func.unix_timestamp(),
        **kwargs):
    """Create a column that is a datetime.datetime In the DB this will be an int
    representing the time since epoch

    NOTA BENE: You probably want to instantiate this class like,

    build_time_column(default_now=True, onupdate_now=True)

    This will make it actually insert default values for the modification time,
    if they're not already specified.

    Arguments:
        raw_column - should the function return a SQLAlchemy.Column? Note that
            This option is incompatible with none_max. We need to explicitly
            define how to compare max date/datetime values when these are store
            as NULL in the DB and the core SQLAlchemy does not permit us to.

    """
    return _build_time_column(
        default_now_value=default_now_value,
        bind_value_functions={
            datetime.datetime: lambda value: int(
                to_timestamp(
                    get_datetime(value)
                )
            ),
            datetime.date: lambda value: int(to_timestamp(value)),
        },
        db_value_to_front_end_value_func=lambda value, _: from_timestamp(
            int(value)
        ),
        raw_column=raw_column,
        **kwargs
    )


def _build_time_column(
        default_now_value,
        bind_value_functions,
        db_value_to_front_end_value_func,
        **kwargs):
    """Convenience function returning a column definition
       and its comparator class.
    """
    raw_column = kwargs.pop('raw_column', False)
    none_max = kwargs.get('none_max')
    if none_max:
        # We need to explicitly define how to compare
        # max date/datetime values when these are store
        # as NULL in the DB, which raw column does not allow to do.
        assert not raw_column, 'Incompatible options: ' \
                               'raw_column=True and none_max=True'

    if kwargs.pop('default_now', False):
        assert kwargs.get('default') is None,\
            "default already specified; default_now" \
            " would override that option."
        kwargs['default'] = default_now_value

    if kwargs.pop('onupdate_now', False):
        kwargs['onupdate'] = default_now_value

    column_type = _make_timestamp_class(
        bind_value_functions=bind_value_functions,
        db_value_to_front_end_value_func=db_value_to_front_end_value_func,
        max_as_none=kwargs.pop('none_max', False),
        maxtime=kwargs.pop('maxtime', None),
        is_max_time_func=kwargs.pop('is_max_time_func', None),
    )

    column, comparator_factory = _get_column_definition(column_type, **kwargs)

    if raw_column:
        return column

    return sqlalchemy.orm.column_property(
        column,
        comparator_factory=comparator_factory
    )


def _get_column_definition(column_type, **kwargs):
    """Helper function that given a column type, returns the sqlalchemy
    column object as well the comparator class."""
    column = sqlalchemy.Column(column_type, **kwargs)
    comparator_factory = column_type.get_column_property_comparator()

    return column, comparator_factory


def _make_timestamp_class(
    bind_value_functions,
    db_value_to_front_end_value_func,
    maxtime=None,
    is_max_time_func=None,
    max_as_none=False,
    implementation_type=sqlalchemy.types.Integer,
    none_db_value=None
):
    """Define a new class to represent time stamps in the DB.

    Args:
        bind_value_functions -- function that takes in a value and binds it
            to its value as store in the DB.

        db_value_to_front_end_value_func -- function that takes in a DB value
            and returns the corresponding timestamp object.

        maxtime -- value for maxtime. Defaults to datetime.datetime.max.

        is_max_time_func -- function that compares a timestamp to maxtime.
            Defaults to a straight comparison with maxtime.

        max_as_none -- Boolean indicating whether maxtime is store as NULL
            in the DB. Defaults to False.

        implementation_type -- Type of the DB Column. Defaults to
            sqlalchemy.types.Integer.

        none_db_value -- Value to set in the database when None is set on the
            column.
    """
    if maxtime is None:
        maxtime = datetime.datetime.max

    class TimeStampTypeClass(_TimeStampType):
        impl = implementation_type
        max_time = maxtime
        bind_to_db_value_functions = bind_value_functions
        store_max_as_none = max_as_none
        _none_db_value = none_db_value

        def _db_value_to_front_end_value(self, value, dialect=None):
            return db_value_to_front_end_value_func(value, dialect)

        def _is_max_time(self, value):
            """Returns True is the input value is max time"""
            if is_max_time_func is not None:
                return is_max_time_func(value, self.max_time)
            return super(TimeStampTypeClass, self)._is_max_time(value)

    return TimeStampTypeClass


class _TimeStampType(sqlalchemy.types.TypeDecorator):
    """Base abstract class defining how time-related
       columns are stored in the database.
    """

    # Type how value are stored in the DB
    impl = None

    # A dictionary of functions that binds values to their
    # DB value representation.
    bind_to_db_value_functions = None

    # value for max time
    max_time = None

    # indicates whether maxtime values are stored as null values in the DB
    store_max_as_none = False

    _none_db_value = None

    def _is_max_time(self, value):
        """Returns True is the input value is identified as max time"""
        return value == self.max_time

    def _db_value_to_front_end_value(self, value, dialect=None):
        """A function that takes in a DB value and returns a well typed
        front-end value."""
        raise NotImplemented

    def process_bind_param(self, value, dialect=None):
        if value is None:
            return self._none_db_value

        if self.store_max_as_none and self._is_max_time(value):
            return None

        bind_func = self.bind_to_db_value_functions.get(type(value))
        if bind_func is None:
            raise TypeError(
                "Unexpected value %r of type %s" % (value, type(value))
            )

        return bind_func(value)

    def process_result_value(self, value, dialect=None):
        if value is None and self.store_max_as_none:
            return self.max_time

        if value is None:
            return None

        return self._db_value_to_front_end_value(value, dialect)

    @classmethod
    def get_column_property_comparator(self):
        if self.store_max_as_none:
            return TimeComparatorNoneMax
        return TimeComparator


class TimeComparator(sqlalchemy.orm.properties.ColumnProperty.Comparator):
    """TimeComparator -- this is only used to add
    the .within function to time columns
    For example:
      this_month = timeuti.CalendarMonthPeriod.current_month()
      models.MyModel.list(
          session, models.MyModel.time_created.within(this_month)
      )
    """

    def within(self, time_period, inclusive=False):
        # We always have a start time
        if inclusive:
            filter_expr = self._column >= time_period.first_time
        else:
            filter_expr = self._column > time_period.first_time

        # Add end_time filter if the last time is not the end of time
        if time_period.last_time != self._max_time:
            if inclusive:
                filter_expr = sqlalchemy.and_(
                    filter_expr,
                    self._column <= time_period.last_time
                )
            else:
                filter_expr = sqlalchemy.and_(
                    filter_expr,
                    self._column < time_period.last_time
                )

        return filter_expr

    @property
    def _max_time(self):
        return self._column.type.max_time

    @property
    def _column(self):
        return self.__clause_element__()


class TimeComparatorNoneMax(TimeComparator):
    """
    Overrides the comparison function to support max time stored as NULL.
    """

    def __eq__(self, other):
        """Compare time objects.

        Time stamps that are set to max_time, aka the end of Time,
        are assumed to be stored as NULL values in the DB when
        this comparator is used.

        """
        if other == self._max_time:
            return super(TimeComparatorNoneMax, self).__eq__(None)
        return super(TimeComparatorNoneMax, self).__eq__(other)


def to_timestamp(datetime_val):
    if datetime_val is None:
        return None

    # If we don't have full datetime granularity, translate
    if isinstance(datetime_val, datetime.datetime):
        datetime_val_date = datetime_val.date()
    else:
        datetime_val_date = datetime_val

    if datetime_val_date >= datetime.date.max:
        return sys.maxsize

    return int(time.mktime(datetime_val.timetuple()))


def from_timestamp(timestamp_val):
    if timestamp_val is None:
        return None
    return datetime.datetime.fromtimestamp(timestamp_val)


def get_datetime(t, preserve_max=False):
    try:
        return to_datetime(t, preserve_max=preserve_max)
    except ValueError:
        return None


def to_datetime(value, preserve_max=False):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    elif isinstance(value, datetime.date):
        return date_to_datetime(value, preserve_max=preserve_max)
    elif isinstance(value, float) or isinstance(value, integer_types):
        return from_timestamp(value)
    raise ValueError("Can't convert %r to a datetime" % (value,))


def date_to_datetime(dt, preserve_max=False):
    if preserve_max and datetime.date.max == dt:
        return datetime.datetime.max
    return datetime.datetime(*dt.timetuple()[:3])
