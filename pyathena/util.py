# -*- coding: utf-8 -*-
import logging
import re
from typing import Any, Callable, Iterable, Optional, Pattern, Tuple

import tenacity
from tenacity import after_log, retry_if_exception, stop_after_attempt, wait_exponential

from pyathena import DataError

_logger = logging.getLogger(__name__)  # type: ignore

PATTERN_OUTPUT_LOCATION: Pattern[str] = re.compile(
    r"^s3://(?P<bucket>[a-zA-Z0-9.\-_]+)/(?P<key>.+)$"
)


def parse_output_location(output_location: str) -> Tuple[str, str]:
    match = PATTERN_OUTPUT_LOCATION.search(output_location)
    if match:
        return match.group("bucket"), match.group("key")
    else:
        raise DataError("Unknown `output_location` format.")


class RetryConfig:
    def __init__(
        self,
        exceptions: Iterable[str] = (
            "ThrottlingException",
            "TooManyRequestsException",
        ),
        attempt: int = 5,
        multiplier: int = 1,
        max_delay: int = 100,
        exponential_base: int = 2,
    ) -> None:
        self.exceptions = exceptions
        self.attempt = attempt
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.exponential_base = exponential_base


def retry_api_call(
    func: Callable[..., Any],
    config: RetryConfig,
    logger: Optional[logging.Logger] = None,
    *args,
    **kwargs,
) -> Any:
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
        after=after_log(logger, logger.level) if logger else None,  # type: ignore
        reraise=True,
    )
    return retry(func, *args, **kwargs)
