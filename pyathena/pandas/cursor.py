# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from pyathena.common import CursorIterator
from pyathena.cursor import BaseCursor
from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaCompression, AthenaFileFormat, AthenaQueryExecution
from pyathena.pandas.converter import (
    DefaultPandasTypeConverter,
    DefaultPandasUnloadTypeConverter,
)
from pyathena.pandas.result_set import AthenaPandasResultSet, DataFrameIterator
from pyathena.result_set import WithResultSet

if TYPE_CHECKING:
    from pandas import DataFrame

_logger = logging.getLogger(__name__)  # type: ignore


class PandasCursor(BaseCursor, CursorIterator, WithResultSet):
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
        unload: bool = False,
        engine: str = "auto",
        chunksize: Optional[int] = None,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        result_reuse_enable: bool = False,
        result_reuse_minutes: int = CursorIterator.DEFAULT_RESULT_REUSE_MINUTES,
        auto_optimize_chunksize: bool = False,
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
        self._unload = unload
        self._engine = engine
        self._chunksize = chunksize
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._auto_optimize_chunksize = auto_optimize_chunksize
        self._on_start_query_execution = on_start_query_execution
        self._query_id: Optional[str] = None
        self._result_set: Optional[AthenaPandasResultSet] = None

    @staticmethod
    def get_default_converter(
        unload: bool = False,
    ) -> Union[DefaultPandasTypeConverter, Any]:
        if unload:
            return DefaultPandasUnloadTypeConverter()
        return DefaultPandasTypeConverter()

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            raise ProgrammingError("arraysize must be a positive integer value.")
        self._arraysize = value

    @property  # type: ignore
    def result_set(self) -> Optional[AthenaPandasResultSet]:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    @query_id.setter
    def query_id(self, val) -> None:
        self._query_id = val

    @property
    def rownumber(self) -> Optional[int]:
        return self.result_set.rownumber if self.result_set else None

    def close(self) -> None:
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
        keep_default_na: bool = False,
        na_values: Optional[Iterable[str]] = ("",),
        quoting: int = 1,
        on_start_query_execution: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> PandasCursor:
        self._reset_state()
        if self._unload:
            s3_staging_dir = s3_staging_dir if s3_staging_dir else self._s3_staging_dir
            assert s3_staging_dir, "If the unload option is used, s3_staging_dir is required."
            operation, unload_location = self._formatter.wrap_unload(
                operation,
                s3_staging_dir=s3_staging_dir,
                format_=AthenaFileFormat.FILE_FORMAT_PARQUET,
                compression=AthenaCompression.COMPRESSION_SNAPPY,
            )
        else:
            unload_location = None
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
            self.result_set = AthenaPandasResultSet(
                connection=self._connection,
                converter=self._converter,
                query_execution=query_execution,
                arraysize=self.arraysize,
                retry_config=self._retry_config,
                keep_default_na=keep_default_na,
                na_values=na_values,
                quoting=quoting,
                unload=self._unload,
                unload_location=unload_location,
                engine=kwargs.pop("engine", self._engine),
                chunksize=kwargs.pop("chunksize", self._chunksize),
                block_size=kwargs.pop("block_size", self._block_size),
                cache_type=kwargs.pop("cache_type", self._cache_type),
                max_workers=kwargs.pop("max_workers", self._max_workers),
                **kwargs,
            )
        else:
            raise OperationalError(query_execution.state_change_reason)

        # Auto-optimize chunksize if enabled and chunksize is set
        if self._auto_optimize_chunksize and self.has_result_set and self._chunksize:
            self._optimize_chunksize()

        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Union[Dict[str, Any], List[str]]]],
        **kwargs,
    ) -> None:
        for parameters in seq_of_parameters:
            self.execute(operation, parameters, **kwargs)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    def cancel(self) -> None:
        if not self.query_id:
            raise ProgrammingError("QueryExecutionId is none or empty.")
        self._cancel(self.query_id)

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.fetchone()

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.fetchmany(size)

    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.fetchall()

    def _optimize_chunksize(self) -> None:
        """Automatically optimize chunksize based on result set characteristics.

        This method optimizes the chunksize for the current result set by:
        1. Analyzing the result file size from S3
        2. Using the result set's auto-determination logic which considers:
           - File size and estimated row count
           - Number of columns in the result
           - Data type complexity (timestamps, decimals, etc.)
        3. Updating the result set's chunksize if a better value is suggested

        The optimization only occurs when:
        - auto_optimize_chunksize is enabled
        - chunksize is already set (not None)
        - A valid result set exists
        - The result file has a determinable size

        This method is automatically called after successful query execution
        and operates silently - any errors in optimization are ignored to
        ensure query execution continues normally.
        """
        if not self.has_result_set:
            return

        result_set = cast(AthenaPandasResultSet, self.result_set)
        try:
            file_size = result_set._get_content_length() or 0
            if file_size > 0:
                # Delegate to result set's chunksize auto-determination logic
                suggested_chunksize = result_set._auto_determine_chunksize(file_size)
                if suggested_chunksize:
                    # Apply the optimized chunksize to the result set
                    result_set._chunksize = suggested_chunksize
        except Exception:
            # Silently ignore optimization errors and continue with current chunksize
            pass

    def as_pandas(self) -> Union["DataFrame", DataFrameIterator]:
        """Return DataFrame or DataFrameIterator based on chunksize setting.

        Returns:
            DataFrame when chunksize is None, DataFrameIterator when chunksize is set.
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")
        result_set = cast(AthenaPandasResultSet, self.result_set)
        return result_set.as_pandas()

    def iter_chunks(self) -> Generator["DataFrame", None, None]:
        """Iterate over DataFrame chunks for memory-efficient processing.

        This method provides an iterator interface for processing large result sets
        in chunks, preventing memory exhaustion when working with datasets that are
        too large to fit in memory as a single DataFrame.

        Yields:
            DataFrame: Individual chunks of the result set when chunksize is set,
                      or the entire DataFrame as a single chunk when chunksize is None.

        Example:
            # Process large result set in chunks
            cursor = connection.cursor(PandasCursor, chunksize=50000)
            cursor.execute("SELECT * FROM large_table")
            for chunk in cursor.iter_chunks():
                # Process chunk
                processed_chunk = chunk.groupby('category').sum()
                # Explicitly delete to free memory
                del chunk
        """
        if not self.has_result_set:
            raise ProgrammingError("No result set.")

        result = self.as_pandas()
        if isinstance(result, DataFrameIterator):
            # It's an iterator (chunked mode)
            import gc

            for chunk_count, chunk in enumerate(result, 1):
                yield chunk

                # Suggest garbage collection every 10 chunks for large datasets
                if chunk_count % 10 == 0:
                    gc.collect()
        else:
            # Single DataFrame - yield as one chunk
            yield result
