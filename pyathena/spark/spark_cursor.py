# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Union, cast

import botocore

from pyathena import NotSupportedError, OperationalError, ProgrammingError
from pyathena.common import BaseCursor
from pyathena.model import (
    AthenaCalculationExecution,
    AthenaQueryExecution,
    AthenaSession,
)
from pyathena.util import retry_api_call

_logger = logging.getLogger(__name__)  # type: ignore


class SparkCursor(BaseCursor):
    def __init__(
        self,
        session_id: Optional[str] = None,
        description: Optional[str] = None,
        engine_configuration: Optional[Dict[str, Any]] = None,
        notebook_version: Optional[str] = None,
        session_idle_timeout_minutes: Optional[int] = None,
        **kwargs,
    ) -> None:
        super(SparkCursor, self).__init__(**kwargs)
        self._engine_configuration = (
            engine_configuration
            if engine_configuration
            else self.get_default_engine_configuration()
        )
        self._notebook_version = notebook_version
        self._session_description = description
        self._session_idle_timeout_minutes = session_idle_timeout_minutes

        if session_id:
            if self._exists_session(session_id):
                self._session_id = session_id
            else:
                raise OperationalError(f"Session: {session_id} not found.")
        else:
            self._session_id = self._start_session()
        self._calculation_id: Optional[str] = None

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def calculation_id(self) -> Optional[str]:
        return self._calculation_id

    @property
    def query_id(self) -> Optional[str]:
        return self.calculation_id

    @query_id.setter
    def query_id(self, val: str) -> None:
        self._calculation_id = val

    @staticmethod
    def get_default_engine_configuration() -> Dict[str, Any]:
        return {
            "CoordinatorDpuSize": 1,
            "MaxConcurrentDpus": 2,
            "DefaultExecutorDpuSize": 1,
        }

    def _get_session_status(self, session_id: str):
        request: Dict[str, Any] = {"SessionId": session_id}
        try:
            response = retry_api_call(
                self._connection.client.get_session_status,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to get session status.")
            raise OperationalError(*e.args) from e
        else:
            return AthenaSession(response)

    def _wait_for_idle_session(self, session_id: str):
        while True:
            session_status = self._get_session_status(session_id)
            if session_status.state in [AthenaSession.STATE_IDLE]:
                break
            elif session_status in [
                AthenaSession.STATE_TERMINATED,
                AthenaSession.STATE_DEGRADED,
                AthenaSession.STATE_FAILED,
            ]:
                raise OperationalError(session_status.state_change_reason)
            else:
                time.sleep(self._poll_interval)

    def _exists_session(self, session_id: str) -> bool:
        request = {"SessionId": session_id}
        try:
            retry_api_call(
                self._connection.client.get_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            if (
                isinstance(e, botocore.exceptions.ClientError)
                and e.response["Error"]["Code"] == "InvalidRequestException"
            ):
                _logger.exception(f"Session: {session_id} not found.")
                return False
            else:
                raise OperationalError(*e.args) from e
        else:
            self._wait_for_idle_session(session_id)
            return True

    def _start_session(self) -> str:
        request: Dict[str, Any] = {
            "WorkGroup": self._work_group,
            "EngineConfiguration": self._engine_configuration,
        }
        if self._session_description:
            request.update({"Description": self._session_description})
        if self._notebook_version:
            request.update({"NotebookVersion": self._notebook_version})
        if self._session_idle_timeout_minutes:
            request.update({"SessionIdleTimeoutInMinutes": self._session_idle_timeout_minutes})
        try:
            session_id: str = retry_api_call(
                self._connection.client.start_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )["SessionId"]
        except Exception as e:
            _logger.exception("Failed to start session.")
            raise OperationalError(*e.args) from e
        else:
            self._wait_for_idle_session(session_id)
            return session_id

    def _terminate_session(self) -> None:
        request = {"SessionId": self._session_id}
        try:
            retry_api_call(
                self._connection.client.terminate_session,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to terminate session.")
            raise OperationalError(*e.args) from e

    def __poll(self, query_id: str) -> Union[AthenaQueryExecution, AthenaCalculationExecution]:
        while True:
            calculation_status = self._get_calculation_execution_status(query_id)
            if calculation_status.state in [
                AthenaCalculationExecution.STATE_COMPLETED,
                AthenaCalculationExecution.STATE_FAILED,
                AthenaCalculationExecution.STATE_CANCELED,
            ]:
                return self._get_calculation_execution(query_id)
            else:
                time.sleep(self._poll_interval)

    def _poll(self, query_id: str) -> Union[AthenaQueryExecution, AthenaCalculationExecution]:
        try:
            query_execution = self.__poll(query_id)
        except KeyboardInterrupt as e:
            if self._kill_on_interrupt:
                _logger.warning("Query canceled by user.")
                self._cancel(query_id)
                query_execution = self.__poll(query_id)
            else:
                raise e
        return query_execution

    def _cancel(self, query_id: str) -> None:
        request = {"CalculationExecutionId": query_id}
        try:
            retry_api_call(
                self._connection.client.stop_calculation_execution,
                config=self._retry_config,
                logger=_logger,
                **request,
            )
        except Exception as e:
            _logger.exception("Failed to cancel calculation.")
            raise OperationalError(*e.args) from e

    def close(self) -> None:
        self._terminate_session()

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
        self.query_id = self._calculate(
            session_id=session_id if session_id else self._session_id,
            code_block=operation,
            description=description,
            client_request_token=client_request_token,
        )
        query_execution = cast(AthenaCalculationExecution, self._poll(self.query_id))
        if query_execution.state != AthenaCalculationExecution.STATE_COMPLETED:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]], **kwargs
    ) -> None:
        raise NotSupportedError

    def cancel(self) -> None:
        if not self.query_id:
            raise ProgrammingError("CalculationExecutionId is none or empty.")
        self._cancel(self.query_id)
