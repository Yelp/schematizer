# -*- coding: utf-8 -*-
"""
:class:`logging.Handler` objects which can be used to send standard python
logging to a scribe stream.

"""

import logging
from clog.loggers import ScribeLogger

from clog.utils import scribify
from clog import global_state


DEFAULT_FORMAT = '%(process)s\t%(asctime)s\t%(name)-12s %(levelname)-8s: %(message)s'


class CLogHandler(logging.Handler):
    """
    .. deprecated:: 0.1.6

    .. warning::

        Use ScribeHandler if you want to log to scribe, or a
        :class:`logging.handlers.FileHandler` to log to a local file.

    Handler for the standard logging library that logs to clog.
    """

    def __init__(self, stream, logger=None):
        'If no logger is specified, the global one is used'
        logging.Handler.__init__(self)
        self.stream = stream
        self.logger = logger or global_state

    def emit(self, record):
        try:
            msg = self.format(record)
            self.logger.log_line(self.stream, msg)
        except Exception:
            raise
        except:
            self.handleError(record)


class ScribeHandler(logging.Handler):
    """Handler for sending python standard logging messages to a scribe
    stream.

    .. code-block:: python

        import clog.handlers, logging
        log = logging.getLogger(name)
        log.addHandler(clog.handlers.ScribeHandler('localhost', 3600, 10))


    :param host: hostname of scribe server
    :param port: port number of scribe server
    :param stream: name of the scribe stream logs will be sent to
    """

    def __init__(self, host, port, stream, retry_interval=None):
        logging.Handler.__init__(self)
        self.stream = stream
        self.logger = ScribeLogger(host, port, retry_interval)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.logger.log_line(self.stream, msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def add_logger_to_scribe(logger, log_level=logging.INFO, fmt=DEFAULT_FORMAT, clogger_object=None):
    """Sets up a logger to log to scribe.

    By default, messages at the INFO level and higher will go to scribe.

    .. deprecated:: 0.1.6

    .. warning::

        This function is deprecated in favor of using :func:`clog.log_line` or
        :class:`ScribeHandler` directly.

    :param logger: A logging.Logger instance
    :param log_level: The level to log at
    :param clogger_object: for use in testing
    """
    scribified_name = scribify(logger.name)
    if any (h.stream == scribified_name for h in logger.handlers if isinstance(h, CLogHandler)):
        return
    clog_handler = CLogHandler(scribified_name, logger=clogger_object)
    clog_handler.setLevel(log_level)
    clog_handler.setFormatter(logging.Formatter(fmt))
    logger.setLevel(log_level)
    logger.addHandler(clog_handler)


def get_scribed_logger(log_name, *args, **kwargs):
    """Get/create a logger and adds it to scribe.

    .. deprecated:: 0.1.6

    .. warning::

        This function is deprecated in favor of using :func:`clog.log_line`
        directly.

    :param log_name: name of log to write to using logging.getLogger
    :param args, kwargs: passed to add_logger_to_scribe
    :returns: a :class:`logging.Logger`
    """
    log = logging.getLogger(log_name)
    add_logger_to_scribe(log, *args, **kwargs)
    return log
