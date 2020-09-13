# -*- coding: utf-8 -*-
import functools
import logging
import re
import threading

import tenacity
from tenacity import after_log, retry_if_exception, stop_after_attempt, wait_exponential

from pyathena import DataError

_logger = logging.getLogger(__name__)

PATTERN_OUTPUT_LOCATION = re.compile(r"^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$")


def parse_output_location(output_location):
    match = PATTERN_OUTPUT_LOCATION.search(output_location)
    if match:
        return match.group("bucket"), match.group("key")
    else:
        raise DataError("Unknown `output_location` format.")


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
    def __init__(
        self,
        exceptions=("ThrottlingException", "TooManyRequestsException"),
        attempt=5,
        multiplier=1,
        max_delay=100,
        exponential_base=2,
    ):
        self.exceptions = exceptions
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def retry_api_call(func, config, logger=None, *args, **kwargs):
    retry = tenacity.Retrying(
        retry=retry_if_exception(
            lambda e: getattr(e, "response", {}).get("Error", {}).get("Code", None)
            in config.exceptions
            if e
            else False
        ),
        stop=stop_after_attempt(config.attempt),
        wait=wait_exponential(
            multiplier=config.multiplier,
            max=config.max_delay,
            exp_base=config.exponential_base,
        ),
        after=after_log(logger, logger.level) if logger else None,
        reraise=True,
    )
    return retry(func, *args, **kwargs)
