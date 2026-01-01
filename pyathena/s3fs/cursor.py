# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import WithResultSet
from pyathena.s3fs.converter import DefaultS3FSTypeConverter
from pyathena.s3fs.result_set import AthenaS3FSResultSet, CSVReaderType

_logger = logging.getLogger(__name__)


class S3FSCursor(BaseCursor, CursorIterator, WithResultSet):
    """Cursor for reading CSV results via S3FileSystem without pandas/pyarrow.

    This cursor uses Python's standard csv module and PyAthena's S3FileSystem
    to read query results from S3. It provides a lightweight alternative to
    pandas and arrow cursors when those dependencies are not needed.

    The cursor is especially useful for:
        - Environments where pandas/pyarrow installation is not desired
        - Simple queries where advanced data processing is not required
        - Memory-constrained environments

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().

    Example:
        >>> from pyathena.s3fs.cursor import S3FSCursor
        >>> cursor = connection.cursor(S3FSCursor)
        >>> cursor.execute("SELECT * FROM my_table")
        >>> rows = cursor.fetchall()  # Returns list of tuples
        >>>
        >>> # Iterate over results
        >>> for row in cursor.execute("SELECT * FROM my_table"):
        ...     print(row)

        # Use with SQLAlchemy
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine("awsathena+s3fs://...")
    """

    def __init__(
        self,
        s3_staging_dir: Optional[str] = None,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        work_group: Optional[str] = None,
        poll_interval: float = 1,
        encryption_option: Optional[str] = None,
        kms_key: Optional[str] = None,
        kill_on_interrupt: bool = True,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        csv_reader: Optional[CSVReaderType] = None,
        **kwargs,
    ) -> None:
        """Initialize an S3FSCursor.

        Args:
            s3_staging_dir: S3 location for query results.
            schema_name: Default schema name.
            catalog_name: Default catalog name.
            work_group: Athena workgroup name.
            poll_interval: Query status polling interval in seconds.
            encryption_option: S3 encryption option (SSE_S3, SSE_KMS, CSE_KMS).
            kms_key: KMS key ARN for encryption.
            kill_on_interrupt: Cancel running query on keyboard interrupt.
            result_reuse_enable: Enable Athena query result reuse.
            result_reuse_minutes: Minutes to reuse cached results.
            on_start_query_execution: Callback invoked when query starts.
            csv_reader: CSV reader class to use for parsing results.
                Use AthenaCSVReader (default) to distinguish between NULL
                (unquoted empty) and empty string (quoted empty "").
                Use DefaultCSVReader for backward compatibility where empty
                strings are treated as NULL.
            **kwargs: Additional connection parameters.

        Example:
            >>> cursor = connection.cursor(S3FSCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>>
            >>> # Use DefaultCSVReader for backward compatibility
            >>> from pyathena.s3fs.reader import DefaultCSVReader
            >>> cursor = connection.cursor(S3FSCursor, csv_reader=DefaultCSVReader)
        """
        super().__init__(
            s3_staging_dir=s3_staging_dir,
            schema_name=schema_name,
            catalog_name=catalog_name,
            work_group=work_group,
            poll_interval=poll_interval,
            encryption_option=encryption_option,
            kms_key=kms_key,
            kill_on_interrupt=kill_on_interrupt,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            **kwargs,
        )
        self._on_start_query_execution = on_start_query_execution
        self._csv_reader = csv_reader
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaS3FSResultSet] = None

    @staticmethod
    def get_default_converter(
        unload: bool = False,  # noqa: ARG004
    ) -> DefaultS3FSTypeConverter:
        """Get the default type converter for S3FS cursor.

        Args:
            unload: Unused. S3FS cursor does not support UNLOAD operations.

        Returns:
            DefaultS3FSTypeConverter instance.
        """
        return DefaultS3FSTypeConverter()

    @property
    def arraysize(self) -> int:
        """Get the number of rows to fetch at a time with fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the number of rows to fetch at a time with fetchmany().

        Args:
            value: Number of rows (must be positive).

        Raises:
            ProgrammingError: If value is not positive.
        """
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    @property  # type: ignore
    def result_set(self) -> Optional[AthenaS3FSResultSet]:
        """Get the current result set."""
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        """Set the current result set."""
        self._result_set = val

    @property
    def query_id(self) -> Optional[str]:
        """Get the ID of the last executed query."""
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        """Set the query ID."""
        self._query_id = val

    @property
    def rownumber(self) -> Optional[int]:
        """Get the current row number (0-indexed)."""
        return self.result_set.rownumber if self.result_set else None

    @property
    def rowcount(self) -> int:
        """Get the number of rows affected by the last operation."""
        return self.result_set.rowcount if self.result_set else -1

    def close(self) -> None:
        """Close the cursor and release resources."""
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

    def execute(
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: Optional[int] = 0,
        cache_expiration_time: Optional[int] = 0,
        result_reuse_enable: Optional[bool] = None,
        result_reuse_minutes: Optional[int] = None,
        paramstyle: Optional[str] = None,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> "S3FSCursor":
        """Execute a SQL query and return results.

        Executes the SQL query on Amazon Athena and configures the result set
        for CSV-based output via S3FileSystem.

        Args:
            operation: SQL query string to execute.
            parameters: Query parameters for parameterized queries.
            work_group: Athena workgroup to use for this query.
            s3_staging_dir: S3 location for query results.
            cache_size: Number of queries to check for result caching.
            cache_expiration_time: Cache expiration time in seconds.
            result_reuse_enable: Enable Athena result reuse for this query.
            result_reuse_minutes: Minutes to reuse cached results.
            paramstyle: Parameter style ('qmark' or 'pyformat').
            on_start_query_execution: Callback called when query starts.
            **kwargs: Additional execution parameters.

        Returns:
            Self reference for method chaining.

        Example:
            >>> cursor.execute("SELECT * FROM my_table WHERE id = %(id)s", {"id": 123})
            >>> rows = cursor.fetchall()
        """
        self._reset_state()
        self.query_id = self._execute(
            operation,
            parameters=parameters,
            work_group=work_group,
            s3_staging_dir=s3_staging_dir,
            cache_size=cache_size,
            cache_expiration_time=cache_expiration_time,
            result_reuse_enable=result_reuse_enable,
            result_reuse_minutes=result_reuse_minutes,
            paramstyle=paramstyle,
        )

        # Call user callbacks immediately after start_query_execution
        if self._on_start_query_execution:
            self._on_start_query_execution(self.query_id)
        if on_start_query_execution:
            on_start_query_execution(self.query_id)

        query_execution = cast(AthenaQueryExecution, self._poll(self.query_id))
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = AthenaS3FSResultSet(
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                csv_reader=self._csv_reader,
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)
        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Union[Dict[str, Any], List[str]]]],
        **kwargs,
    ) -> None:
        """Execute a SQL query with multiple parameter sets.

        Args:
            operation: SQL query string to execute.
            seq_of_parameters: Sequence of parameter sets.
            **kwargs: Additional execution parameters.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters, **kwargs)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    def cancel(self) -> None:
        """Cancel the currently running query.

        Raises:
            ProgrammingError: If no query is running.
        """
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self.query_id)

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of the result set.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If no query has been executed.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaS3FSResultSet, self.result_set)
        return result_set.fetchone()

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next set of rows of the result set.

        Args:
            size: Maximum number of rows to fetch. Defaults to arraysize.

        Returns:
            A list of tuples representing the rows.

        Raises:
            ProgrammingError: If no query has been executed.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaS3FSResultSet, self.result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows of the result set.

        Returns:
            A list of tuples representing all remaining rows.

        Raises:
            ProgrammingError: If no query has been executed.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaS3FSResultSet, self.result_set)
        return result_set.fetchall()
