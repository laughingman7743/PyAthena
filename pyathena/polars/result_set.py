# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
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
        chunksize: Optional[int] = None,
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
            chunksize: Number of rows per chunk for memory-efficient processing.
                      If specified, enables chunked iteration via iter_chunks().
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
        self._chunksize = chunksize
        self._kwargs = kwargs
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            # Skip eager loading when chunksize is set to avoid double reads
            # User should use iter_chunks() for memory-efficient processing
            if self._chunksize is None:
                self._df = self._as_polars()
            else:
                import polars as pl

                self._df = pl.DataFrame()
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

    def _is_csv_readable(self) -> bool:
        """Check if CSV output is available and can be read.

        Returns:
            True if CSV data is available to read, False otherwise.

        Raises:
            ProgrammingError: If output location is not set.
        """
        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        if not self.output_location.endswith((".csv", ".txt")):
            return False
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return False
        length = self._get_content_length()
        return length != 0

    def _prepare_parquet_location(self) -> bool:
        """Prepare unload location for Parquet reading.

        Returns:
            True if Parquet data is available to read, False otherwise.
        """
        manifests = self._read_data_manifest()
        if not manifests:
            return False
        if not self._unload_location:
            self._unload_location = "/".join(manifests[0].split("/")[:-1]) + "/"
        return True

    def _read_csv(self) -> "pl.DataFrame":
        """Read query results from CSV file in S3.

        Returns:
            Polars DataFrame containing the CSV data.

        Raises:
            ProgrammingError: If output location is not set.
            OperationalError: If reading the CSV file fails.
        """
        import polars as pl

        if not self._is_csv_readable():
            return pl.DataFrame()

        # After validation, output_location is guaranteed to be set
        assert self.output_location is not None

        separator, has_header, new_columns = self._get_csv_params()

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

        if not self._prepare_parquet_location():
            return pl.DataFrame()

        # After preparation, unload_location is guaranteed to be set
        assert self._unload_location is not None

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

    def _get_csv_params(self) -> Tuple[str, bool, Optional[List[str]]]:
        """Get CSV parsing parameters based on file type.

        Returns:
            Tuple of (separator, has_header, new_columns).
        """
        if self.output_location and self.output_location.endswith(".txt"):
            separator = "\t"
            has_header = False
            description = self.description if self.description else []
            new_columns: Optional[List[str]] = [d[0] for d in description]
        else:
            separator = ","
            has_header = True
            new_columns = None
        return separator, has_header, new_columns

    def _iter_csv_chunks(self) -> Iterator["pl.DataFrame"]:
        """Iterate over CSV data in chunks using lazy evaluation.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            ProgrammingError: If output location is not set.
            OperationalError: If reading the CSV file fails.
        """
        import polars as pl

        if not self._is_csv_readable():
            return

        # After validation, output_location is guaranteed to be set
        assert self.output_location is not None

        separator, has_header, new_columns = self._get_csv_params()

        try:
            # scan_csv uses Rust's native object_store (like scan_parquet),
            # not fsspec, so we use the same storage options as Parquet
            lazy_df = pl.scan_csv(
                self.output_location,
                separator=separator,
                has_header=has_header,
                schema_overrides=self.dtypes,
                storage_options=self._parquet_storage_options,
                **self._kwargs,
            )
            for batch in lazy_df.collect_batches(chunk_size=self._chunksize):
                if new_columns:
                    batch.columns = new_columns
                yield batch
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _iter_parquet_chunks(self) -> Iterator["pl.DataFrame"]:
        """Iterate over Parquet data in chunks using lazy evaluation.

        Yields:
            Polars DataFrame for each chunk.

        Raises:
            OperationalError: If reading the Parquet files fails.
        """
        import polars as pl

        if not self._prepare_parquet_location():
            return

        # After preparation, unload_location is guaranteed to be set
        assert self._unload_location is not None

        try:
            lazy_df = pl.scan_parquet(
                self._unload_location,
                storage_options=self._parquet_storage_options,
                **self._kwargs,
            )
            for batch in lazy_df.collect_batches(chunk_size=self._chunksize):
                yield batch
        except Exception as e:
            _logger.exception(f"Failed to read {self._unload_location}.")
            raise OperationalError(*e.args) from e

    def iter_chunks(self) -> Iterator["pl.DataFrame"]:
        """Iterate over result chunks as Polars DataFrames.

        This method provides an iterator interface for processing large result sets.
        When chunksize is specified, it yields DataFrames in chunks using lazy
        evaluation for memory-efficient processing. When chunksize is not specified,
        it yields the entire result as a single DataFrame.

        Yields:
            Polars DataFrame for each chunk of rows, or the entire DataFrame
            if chunksize was not specified.

        Example:
            >>> # With chunking for large datasets
            >>> cursor = connection.cursor(PolarsCursor, chunksize=50000)
            >>> cursor.execute("SELECT * FROM large_table")
            >>> for chunk in cursor.iter_chunks():
            ...     process_chunk(chunk)  # Each chunk is a Polars DataFrame
            >>>
            >>> # Without chunking - yields entire result as single chunk
            >>> cursor = connection.cursor(PolarsCursor)
            >>> cursor.execute("SELECT * FROM small_table")
            >>> for df in cursor.iter_chunks():
            ...     process(df)  # Single DataFrame with all data
        """
        if self._chunksize is None:
            # No chunking - yield entire DataFrame as single chunk
            yield self._df
        elif self.is_unload:
            yield from self._iter_parquet_chunks()
        else:
            yield from self._iter_csv_chunks()

    def close(self) -> None:
        """Close the result set and release resources."""
        import polars as pl

        super().close()
        self._df = pl.DataFrame()
        self._row_index = 0
