# This is the uWSGI-specific part of uwsgi_metrics.

import contextlib
import logging
import marshal
import mmap
import time

try:
    import uwsgi
except ImportError:
    # The uwsgi module is only available when running under uWSGI.
    # Mock it out for tests and documentation building.
    class uwsgi(object):
        @staticmethod
        def lock():
            pass

        @staticmethod
        def unlock():
            pass

        @staticmethod
        def add_timer(signal, period):
            pass

        @staticmethod
        def register_signal(signal, target, func):
            pass


try:
    import uwsgidecorators
except ImportError:
    class uwsgidecorators(object):
        @staticmethod
        def mulefunc(target):
            def decorator(fun):
                return fun
            return decorator


from uwsgi_metrics.__about__ import __version__
from uwsgi_metrics.counter import Counter
from uwsgi_metrics.histogram import Histogram
from uwsgi_metrics.meter import Meter
from uwsgi_metrics.timer import Timer

DEFAULT_UPDATE_PERIOD_S = 5
DEFAULT_TIMER_SIGNAL_NUMBER = 42
MAX_MARSHALLED_VIEW_SIZE = 2**20
MULE = 'mule1'

log = logging.getLogger('uwsgi_metrics.metrics')

# Map of all metrics of type: metric_type -> module -> metric_name -> metric
# where 'module' and 'metric_name' are both string values.
# These metrics are periodically marshalled to the memory mapped buffer for
# viewing by regular workers.
all_metrics = {}

# The memory mapped buffer
marshalled_metrics_mmap = mmap.mmap(-1, MAX_MARSHALLED_VIEW_SIZE)
marshalled_metrics_mmap.write(marshal.dumps({
    'version': __version__,
    'counters': {},
    'gauges': {},
    'histograms': {},
    'meters': {},
    'timers': {},
}))

# Set when initialized() has been invoked
initialized = False


class NotInitialized(Exception):
    """Raised when the initialize() method has not been invoked."""


def get_metric(ty, module, name, default):
    key = (ty, module, name)
    return all_metrics.setdefault(key, default)


def initialize(signal_number=DEFAULT_TIMER_SIGNAL_NUMBER,
               update_period_s=DEFAULT_UPDATE_PERIOD_S):
    """Initialize metrics, must be invoked at least once prior to invoking any
    other method."""
    global initialized
    if initialized:
        return
    initialized = True
    uwsgi.add_timer(signal_number, update_period_s)
    uwsgi.register_signal(signal_number, MULE, emit)


def reset():
    """Test-only method"""
    global all_metrics, initialized
    initialized = False
    all_metrics = {}


def emit(_):
    """Serialize metrics to the memory mapped buffer."""
    if not initialized:
        raise NotInitialized

    view = {
        'version': __version__,
        'counters': {},
        'gauges': {},
        'histograms': {},
        'meters': {},
        'timers': {},
    }

    for (ty, module, name), metric in all_metrics.iteritems():
        view[ty]['%s.%s' % (module, name)] = metric.view()

    marshalled_view = marshal.dumps(view)
    if len(marshalled_view) > MAX_MARSHALLED_VIEW_SIZE:
        log.warn(
            'Marshalled length too large, got %d, max %d. '
            'Try recording fewer metrics or increasing '
            'MAX_MARSHALLED_VIEW_SIZE'
            % (len(marshalled_view), MAX_MARSHALLED_VIEW_SIZE))
        return
    marshalled_metrics_mmap.seek(0)
    try:
        # Reading and writing to/from an mmap'ed buffer is not guaranteed
        # to be atomic, so we must serialize access to it.
        uwsgi.lock()
        marshalled_metrics_mmap.write(marshalled_view)
    finally:
        uwsgi.unlock()


def view():
    """Get a dictionary representation of current metrics."""
    if not initialized:
        raise NotInitialized

    marshalled_metrics_mmap.seek(0)
    try:
        uwsgi.lock()
        marshalled_view = marshalled_metrics_mmap.read(
            MAX_MARSHALLED_VIEW_SIZE)
    finally:
        uwsgi.unlock()
    return marshal.loads(marshalled_view)


@contextlib.contextmanager
def timing(module, name):
    """
    Context manager to time a section of code::

        with timing(__name__, 'my_timer'):
            do_some_operation()
    """
    start_time_s = time.time()
    yield
    end_time_s = time.time()
    delta_s = end_time_s - start_time_s
    delta_ms = delta_s * 1000
    timer(module, name, delta_ms)


@uwsgidecorators.mulefunc(1)
def timer(module, name, delta, duration_units='milliseconds'):
    """
    Record a timing delta:
    ::

        start_time_s = time.time()
        do_some_operation()
        end_time_s = time.time()
        delta_s = end_time_s - start_time_s
        delta_ms = delta_s * 1000
        timer(__name__, 'my_timer', delta_ms)
    """
    timer = get_metric('timers', module, name, Timer(duration_units))
    timer.update(delta)


@uwsgidecorators.mulefunc(1)
def histogram(module, name, value):
    """
    Record a value in a histogram:
    ::

        histogram(__name__, 'my_histogram', len(queue))
    """
    histogram = get_metric('histograms', module, name, Histogram())
    histogram.update(value)


@uwsgidecorators.mulefunc(1)
def counter(module, name, count=1):
    """
    Record an event's occurence in a counter:
    ::

       counter(__name__, 'my_counter')
    """
    counter = get_metric('counters', module, name, Counter())
    counter.inc(count)


@uwsgidecorators.mulefunc(1)
def meter(module, name, count=1):
    """
    Record an event rate:
    ::

       meter(__name__, 'my_meter', 'event_type')
    """
    meter = get_metric('meters', module, name, Meter())
    meter.mark(count)
