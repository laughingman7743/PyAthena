# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

from pyathena.common import BaseCursor, CursorIterator
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaDictResultSet, AthenaResultSet, WithResultSet

_logger = logging.getLogger(__name__)  # type: ignore


class Cursor(BaseCursor, CursorIterator, WithResultSet):
    """A DB API 2.0 compliant cursor for executing SQL queries on Amazon Athena.

    The Cursor class provides methods for executing SQL queries against Amazon Athena
    and retrieving results. It follows the Python Database API Specification v2.0
    (PEP 249) and provides familiar database cursor operations.

    This cursor returns results as tuples by default. For other data formats,
    consider using specialized cursor classes like PandasCursor or ArrowCursor.

    Attributes:
        description: Sequence of column descriptions for the last query.
        rowcount: Number of rows affected by the last query (-1 for SELECT queries).
        arraysize: Default number of rows to fetch with fetchmany().

    Example:
        >>> cursor = connection.cursor()
        >>> cursor.execute("SELECT name, age FROM users WHERE age > %s", (18,))
        >>> while True:
        ...     row = cursor.fetchone()
        ...     if not row:
        ...         break
        ...     print(f"Name: {row[0]}, Age: {row[1]}")

        >>> cursor.execute("CREATE TABLE test AS SELECT 1 as id, 'test' as name")
        >>> print(f"Created table, rows affected: {cursor.rowcount}")
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
        **kwargs,
    ) -> None:
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
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaResultSet] = None
        self._result_set_class = AthenaResultSet
        self._on_start_query_execution = on_start_query_execution

    @property
    def result_set(self) -> Optional[AthenaResultSet]:
        """Get the result set from the last executed query.

        Returns:
            The result set object containing query results, or None if no
            query has been executed or the query didn't return results.
        """
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        """Set the result set for the cursor.

        Args:
            val: The result set object to assign.
        """
        self._result_set = val

    @property
    def query_id(self) -> Optional[str]:
        """Get the Athena query execution ID of the last executed query.

        Returns:
            The query execution ID assigned by Athena, or None if no query
            has been executed.
        """
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        """Set the Athena query execution ID.

        Args:
            val: The query execution ID to set.
        """
        self._query_id = val

    @property
    def rownumber(self) -> Optional[int]:
        """Get the current row number within the result set.

        Returns:
            The zero-based index of the current row, or None if no result set
            is available or no rows have been fetched.
        """
        return self.result_set.rownumber if self.result_set else None

    @property
    def rowcount(self) -> int:
        """Get the number of rows affected by the last operation.

        For SELECT statements, this returns the total number of rows in the
        result set. For other operations, behavior follows DB API 2.0 specification.

        Returns:
            The number of rows, or -1 if not applicable or unknown.
        """
        return self.result_set.rowcount if self.result_set else -1

    def close(self) -> None:
        """Close the cursor and free any associated resources.

        Closes the cursor and any associated result sets. This method is provided
        for DB API 2.0 compatibility and should be called when the cursor is no
        longer needed.

        Note:
            After calling this method, the cursor should not be used for
            further database operations.
        """
        if self.result_set and not self.result_set.is_closed:
            self.result_set.close()

    def execute(
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], List[str]]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        result_reuse_enable: Optional[bool] = None,
        result_reuse_minutes: Optional[int] = None,
        paramstyle: Optional[str] = None,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> Cursor:
        """Execute a SQL query.

        Args:
            operation: SQL query string to execute
            parameters: Query parameters (optional)
            on_start_query_execution: Callback function called immediately after
                                    start_query_execution API is called.
                                    Function signature: (query_id: str) -> None
                                    This allows early access to query_id for
                                    monitoring/cancellation.
            **kwargs: Additional execution parameters

        Returns:
            Cursor: Self reference for method chaining

        Example with callback for early query ID access:
            def on_execution_started(query_id):
                print(f"Query execution started: {query_id}")
                # Store query_id for potential cancellation from another thread
                global current_query_id
                current_query_id = query_id

            cursor.execute("SELECT * FROM large_table",
                         on_start_query_execution=on_execution_started)
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
        # Both connection-level and execute-level callbacks are invoked if set
        if self._on_start_query_execution:
            self._on_start_query_execution(self.query_id)
        if on_start_query_execution:
            on_start_query_execution(self.query_id)

        query_execution = cast(AthenaQueryExecution, self._poll(self.query_id))
        if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
            self.result_set = self._result_set_class(
                self._connection,
                self._converter,
                query_execution,
                self.arraysize,
                self._retry_config,
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
        """Execute a SQL query multiple times with different parameters.

        This method executes the same SQL operation multiple times, once for each
        parameter set in the sequence. This is useful for bulk operations like
        inserting multiple rows.

        Args:
            operation: SQL query string to execute.
            seq_of_parameters: Sequence of parameter dictionaries or lists, one for each execution.
            **kwargs: Additional keyword arguments passed to each execute() call.

        Note:
            This method executes each query sequentially. For better performance
            with bulk operations, consider using batch operations where supported.
            Operations that return result sets are not allowed with executemany.

        Example:
            >>> cursor.executemany(
            ...     "INSERT INTO users (id, name) VALUES (%(id)s, %(name)s)",
            ...     [
            ...         {"id": 1, "name": "Alice"},
            ...         {"id": 2, "name": "Bob"},
            ...         {"id": 3, "name": "Charlie"}
            ...     ]
            ... )
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters, **kwargs)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    def cancel(self) -> None:
        """Cancel the currently executing query.

        Cancels the query execution on Amazon Athena. This method can be called
        from a different thread to interrupt a long-running query.

        Raises:
            ProgrammingError: If no query is currently executing (query_id is None).

        Example:
            >>> import threading
            >>> import time
            >>>
            >>> def cancel_after_delay():
            ...     time.sleep(5)  # Wait 5 seconds
            ...     cursor.cancel()
            >>>
            >>> # Start cancellation in separate thread
            >>> threading.Thread(target=cancel_after_delay).start()
            >>> cursor.execute("SELECT * FROM very_large_table")
        """
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self.query_id)

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of a query result set.

        Returns the next row of the query result as a tuple, or None when
        no more data is available. Column values are converted to appropriate
        Python types based on the Athena data types.

        Returns:
            A tuple representing the next row, or None if no more rows.

        Raises:
            ProgrammingError: If called before executing a query that returns results.

        Example:
            >>> cursor.execute("SELECT id, name FROM users LIMIT 3")
            >>> while True:
            ...     row = cursor.fetchone()
            ...     if not row:
            ...         break
            ...     print(f"ID: {row[0]}, Name: {row[1]}")
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchone()

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch multiple rows from a query result set.

        Returns up to 'size' rows from the query result as a list of tuples.
        If size is not specified, uses the cursor's arraysize attribute.

        Args:
            size: Maximum number of rows to fetch. If None, uses arraysize.

        Returns:
            List of tuples representing the fetched rows. May contain fewer
            rows than requested if fewer are available.

        Raises:
            ProgrammingError: If called before executing a query that returns results.

        Example:
            >>> cursor.execute("SELECT id, name FROM users")
            >>> rows = cursor.fetchmany(5)  # Fetch up to 5 rows
            >>> for row in rows:
            ...     print(f"ID: {row[0]}, Name: {row[1]}")
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows from a query result set.

        Returns all remaining rows from the query result as a list of tuples.
        For large result sets, consider using fetchmany() or iterating with
        fetchone() to avoid memory issues.

        Returns:
            List of tuples representing all remaining rows in the result set.

        Raises:
            ProgrammingError: If called before executing a query that returns results.

        Example:
            >>> cursor.execute("SELECT id, name FROM users WHERE active = true")
            >>> all_rows = cursor.fetchall()
            >>> print(f"Found {len(all_rows)} active users")
            >>> for row in all_rows:
            ...     print(f"ID: {row[0]}, Name: {row[1]}")

        Warning:
            Be cautious with large result sets as this loads all data into memory.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaResultSet, self.result_set)
        return result_set.fetchall()


class DictCursor(Cursor):
    """A cursor that returns query results as dictionaries instead of tuples.

    DictCursor provides the same functionality as the standard Cursor but
    returns rows as dictionaries where column names are keys. This makes
    it easier to access column values by name rather than position.

    Example:
        >>> cursor = connection.cursor(DictCursor)
        >>> cursor.execute("SELECT id, name, email FROM users LIMIT 1")
        >>> row = cursor.fetchone()
        >>> print(f"User: {row['name']} ({row['email']})")

        >>> cursor.execute("SELECT * FROM products")
        >>> for row in cursor.fetchall():
        ...     print(f"Product {row['id']}: {row['name']} - ${row['price']}")
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._result_set_class = AthenaDictResultSet
        if "dict_type" in kwargs:
            AthenaDictResultSet.dict_type = kwargs["dict_type"]
