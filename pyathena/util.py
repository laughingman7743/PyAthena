# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import functools
import threading

import tenacity
from tenacity import (after_log, retry_if_exception,
                      stop_after_attempt, wait_exponential)


def as_pandas(cursor):
    from pandas import DataFrame
    names = [metadata[0] for metadata in cursor.description]
    return DataFrame.from_records(cursor.fetchall(), columns=names)


def synchronized(wrapped):
    """The missing @synchronized decorator

    https://git.io/vydTA"""
    _lock = threading.RLock()

    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        with _lock:
            return wrapped(*args, **kwargs)
    return _wrapper


class RetryConfig(object):

    def __init__(self, exceptions=('ThrottlingException', 'TooManyRequestsException'),
                 attempt=5, multiplier=1, max_delay=100, exponential_base=2):
        self.exceptions = exceptions
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def retry_api_call(func, config, logger=None,
                   *args, **kwargs):
    retry = tenacity.Retrying(
        retry=retry_if_exception(
            lambda e: getattr(e, 'response', {}).get(
                'Error', {}).get('Code', None) in config.exceptions
            if e else False),
        stop=stop_after_attempt(config.attempt),
        wait=wait_exponential(multiplier=config.multiplier,
                              max=config.max_delay,
                              exp_base=config.exponential_base),
        after=after_log(logger, logger.level) if logger else None,
        reraise=True
    )
    return retry(func, *args, **kwargs)
