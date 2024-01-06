# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, cast

from pyathena import OperationalError, ProgrammingError
from pyathena.model import AthenaCalculationExecution
from pyathena.spark.common import SparkBaseCursor, WithCalculationExecution

_logger = logging.getLogger(__name__)  # type: ignore


class SparkCursor(SparkBaseCursor, WithCalculationExecution):
    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    @property
    def calculation_execution(self) -> Optional[AthenaCalculationExecution]:
        return self._calculation_execution

    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        client_request_token: Optional[str] = None,
        work_group: Optional[str] = None,
        **kwargs,
    ) -> SparkCursor:
        calculation_id = self._calculate(
            session_id=session_id if session_id else self._session_id,
            code_block=operation,
            description=description,
            client_request_token=client_request_token,
        )
        self._calculation_execution = cast(AthenaCalculationExecution, self._poll(calculation_id))
        if self._calculation_execution.state != AthenaCalculationExecution.STATE_COMPLETED:
            raise OperationalError(self._calculation_execution.state_change_reason)
        return self

    def cancel(self) -> None:
        if not self.calculation_id:
            raise ProgrammingError("CalculationExecutionId is none or empty.")
        self._cancel(self.calculation_id)
