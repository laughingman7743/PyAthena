# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from pyathena import OperationalError
from pyathena.converter import Converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.polars.util import to_column_info
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig

if TYPE_CHECKING:
    import polars as pl
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)


class AthenaPolarsResultSet(AthenaResultSet):
    """Result set that provides Polars DataFrame results with optional Arrow interoperability.

    This result set handles CSV and Parquet result files from S3, converting them to
    Polars DataFrames using Polars' native reading capabilities. It does not require
    PyArrow for basic functionality, but can optionally provide Arrow Table access
    when PyArrow is installed.

    Features:
        - Native Polars CSV and Parquet reading (no PyArrow required)
        - Efficient columnar data processing with Polars
        - Optional Arrow interoperability when PyArrow is available
        - Support for both CSV and Parquet result formats
        - Optimized memory usage through columnar format

    Example:
        >>> # Used automatically by PolarsCursor
        >>> cursor = connection.cursor(PolarsCursor)
        >>> cursor.execute("SELECT * FROM large_table")
        >>>
        >>> # Get Polars DataFrame
        >>> df = cursor.as_polars()
        >>>
        >>> # Work with Polars
        >>> print(f"DataFrame has {df.height} rows and {df.width} columns")
        >>> filtered = df.filter(pl.col("value") > 100)
        >>>
        >>> # Optional: Get Arrow Table (requires pyarrow)
        >>> table = cursor.as_arrow()

    Note:
        This class is used internally by PolarsCursor and typically not
        instantiated directly by users. Requires polars to be installed.
        PyArrow is optional and only needed for as_arrow() functionality.
    """

    def __init__(
        self,
        connection: "Connection[Any]",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        unload: bool = False,
        unload_location: Optional[str] = None,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        **kwargs,
    ) -> None:
        """Initialize the Polars result set.

        Args:
            connection: The Athena connection object.
            converter: Type converter for Athena data types.
            query_execution: Query execution metadata.
            arraysize: Number of rows to fetch per batch.
            retry_config: Configuration for retry behavior.
            unload: Whether this is an UNLOAD query result.
            unload_location: S3 location for UNLOAD results.
            block_size: Block size for S3 file reading.
            cache_type: Cache type for S3 file system.
            max_workers: Maximum number of worker threads.
            **kwargs: Additional arguments passed to Polars read functions.
        """
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
        )
        self._rows.clear()  # Clear pre_fetch data
        self._arraysize = arraysize
        self._unload = unload
        self._unload_location = unload_location
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._kwargs = kwargs
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            self._df = self._as_polars()
        else:
            import polars as pl

            self._df = pl.DataFrame()
        self._row_index = 0

    @property
    def _csv_storage_options(self) -> Dict[str, Any]:
        """Get storage options for Polars CSV reading via fsspec.

        Polars read_csv uses fsspec for cloud storage access, which works
        with PyAthena's registered S3FileSystem.

        Returns:
            Dictionary with fsspec-compatible options for S3 access.
        """
        return {
            "connection": self.connection,
            "default_block_size": self._block_size,
            "default_cache_type": self._cache_type,
            "max_workers": self._max_workers,
        }

    @property
    def _parquet_storage_options(self) -> Dict[str, Any]:
        """Get storage options for Polars Parquet reading via native object_store.

        Polars read_parquet uses Rust's native object_store crate, which requires
        AWS credentials to be passed directly rather than through fsspec.

        Returns:
            Dictionary with AWS credentials and region for S3 access.
        """
        credentials = self.connection.session.get_credentials()
        options: Dict[str, Any] = {}
        if credentials:
            frozen_credentials = credentials.get_frozen_credentials()
            options["aws_access_key_id"] = frozen_credentials.access_key
            options["aws_secret_access_key"] = frozen_credentials.secret_key
            if frozen_credentials.token:
                options["aws_session_token"] = frozen_credentials.token
        if self.connection.region_name:
            options["aws_region"] = self.connection.region_name
        return options

    @property
    def dtypes(self) -> Dict[str, Any]:
        """Get Polars-compatible data types for result columns."""
        description = self.description if self.description else []
        return {
            d[0]: dtype
            for d in description
            if (dtype := self._converter.get_dtype(d[1], d[4], d[5])) is not None
        }

    @property
    def converters(self) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        """Get converter functions for each column.

        Returns:
            Dictionary mapping column names to their converter functions.
        """
        description = self.description if self.description else []
        return {d[0]: self._converter.get(d[1]) for d in description}

    def _fetch(self) -> None:
        """Fetch rows from the DataFrame into the row buffer."""
        if self._row_index >= self._df.height:
            return

        end_index = min(self._row_index + self._arraysize, self._df.height)
        chunk = self._df.slice(self._row_index, end_index - self._row_index)
        self._row_index = end_index

        # Convert to rows and apply converters
        description = self.description if self.description else []
        column_names = [d[0] for d in description]
        for row_dict in chunk.iter_rows(named=True):
            processed_row = tuple(
                self.converters.get(col, lambda x: x)(row_dict.get(col)) for col in column_names
            )
            self._rows.append(processed_row)

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next row of the query result.

        Returns:
            A single row as a tuple, or None if no more rows are available.
        """
        if not self._rows:
            self._fetch()
        if not self._rows:
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        return self._rows.popleft()

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch the next set of rows of the query result.

        Args:
            size: Number of rows to fetch. Defaults to arraysize.

        Returns:
            A list of rows as tuples.
        """
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def fetchall(
        self,
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        """Fetch all remaining rows of the query result.

        Returns:
            A list of all remaining rows as tuples.
        """
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _read_csv(self) -> "pl.DataFrame":
        """Read query results from CSV file in S3.

        Returns:
            Polars DataFrame containing the CSV data.

        Raises:
            ProgrammingError: If output location is not set.
            OperationalError: If reading the CSV file fails.
        """
        import polars as pl

        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        if not self.output_location.endswith((".csv", ".txt")):
            return pl.DataFrame()
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return pl.DataFrame()
        length = self._get_content_length()
        if length == 0:
            return pl.DataFrame()

        if self.output_location.endswith(".txt"):
            separator = "\t"
            has_header = False
            description = self.description if self.description else []
            new_columns = [d[0] for d in description]
        elif self.output_location.endswith(".csv"):
            separator = ","
            has_header = True
            new_columns = None
        else:
            return pl.DataFrame()

        try:
            df = pl.read_csv(
                self.output_location,
                separator=separator,
                has_header=has_header,
                schema_overrides=self.dtypes,
                storage_options=self._csv_storage_options,
                **self._kwargs,
            )
            if new_columns:
                df.columns = new_columns
            return df
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet(self) -> "pl.DataFrame":
        """Read query results from Parquet files in S3.

        Returns:
            Polars DataFrame containing the Parquet data.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        manifests = self._read_data_manifest()
        if not manifests:
            return pl.DataFrame()
        if not self._unload_location:
            self._unload_location = "/".join(manifests[0].split("/")[:-1]) + "/"

        try:
            return pl.read_parquet(
                self._unload_location,
                storage_options=self._parquet_storage_options,
                **self._kwargs,
            )
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet_schema(self) -> Tuple[Dict[str, Any], ...]:
        """Read schema from Parquet files for metadata."""
        import polars as pl

        if not self._unload_location:
            raise ProgrammingError("UnloadLocation is none or empty.")

        try:
            # Use scan_parquet to get schema without reading all data
            lazy_df = pl.scan_parquet(
                self._unload_location,
                storage_options=self._parquet_storage_options,
            )
            schema = lazy_df.collect_schema()
            return to_column_info(schema)
        except Exception as e:
            _logger.exception(f"Failed to read schema from {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def _as_polars(self) -> "pl.DataFrame":
        """Load query results as a Polars DataFrame.

        Reads from Parquet for UNLOAD queries, otherwise from CSV.

        Returns:
            Polars DataFrame containing the query results.
        """
        if self.is_unload:
            df = self._read_parquet()
            if df.is_empty():
                self._metadata = ()
            else:
                self._metadata = self._read_parquet_schema()
        else:
            df = self._read_csv()
        return df

    def as_polars(self) -> "pl.DataFrame":
        """Return query results as a Polars DataFrame.

        Returns the query results as a Polars DataFrame. This is the primary
        method for accessing results with PolarsCursor.

        Returns:
            Polars DataFrame containing all query results.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> df = cursor.as_polars()
            >>> print(f"DataFrame has {df.height} rows")
            >>> filtered = df.filter(pl.col("value") > 100)
        """
        return self._df

    def as_arrow(self) -> "Table":
        """Return query results as an Apache Arrow Table.

        Converts the Polars DataFrame to an Apache Arrow Table for
        interoperability with other Arrow-compatible tools and libraries.

        Returns:
            Apache Arrow Table containing all query results.

        Raises:
            ImportError: If pyarrow is not installed.

        Example:
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM my_table")
            >>> table = cursor.as_arrow()
            >>> # Use with other Arrow-compatible libraries
        """
        try:
            return self._df.to_arrow()
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for as_arrow(). Install it with: pip install pyarrow"
            ) from e

    def close(self) -> None:
        """Close the result set and release resources."""
        import polars as pl

        super().close()
        self._df = pl.DataFrame()
        self._row_index = 0
