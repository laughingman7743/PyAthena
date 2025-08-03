# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from collections import abc
from multiprocessing import cpu_count
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from pyathena import OperationalError
from pyathena.converter import Converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig, parse_output_location

if TYPE_CHECKING:
    from pandas import DataFrame
    from pandas.io.parsers import TextFileReader

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


def _no_trunc_date(df: "DataFrame") -> "DataFrame":
    return df


class DataFrameIterator(abc.Iterator):  # type: ignore
    def __init__(
        self,
        reader: Union["TextFileReader", "DataFrame"],
        trunc_date: Callable[["DataFrame"], "DataFrame"],
    ) -> None:
        from pandas import DataFrame

        if isinstance(reader, DataFrame):
            self._reader = iter([reader])
        else:
            self._reader = reader
        self._trunc_date = trunc_date

    def __next__(self):
        try:
            df = next(self._reader)
            return self._trunc_date(df)
        except StopIteration:
            self.close()
            raise

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self) -> None:
        from pandas.io.parsers import TextFileReader

        if isinstance(self._reader, TextFileReader):
            self._reader.close()

    def iterrows(self) -> Iterator[Any]:
        for df in self:
            for row in enumerate(df.to_dict("records")):
                yield row

    def get_chunk(self, size=None):
        from pandas.io.parsers import TextFileReader

        if isinstance(self._reader, TextFileReader):
            return self._reader.get_chunk(size)
        return next(self._reader)


class AthenaPandasResultSet(AthenaResultSet):
    _parse_dates: List[str] = [
        "date",
        "time",
        "time with time zone",
        "timestamp",
        "timestamp with time zone",
    ]

    def __init__(
        self,
        connection: "Connection[Any]",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        keep_default_na: bool = False,
        na_values: Optional[Iterable[str]] = ("",),
        quoting: int = 1,
        unload: bool = False,
        unload_location: Optional[str] = None,
        engine: str = "auto",
        chunksize: Optional[int] = None,
        block_size: Optional[int] = None,
        cache_type: Optional[str] = None,
        max_workers: int = (cpu_count() or 1) * 5,
        **kwargs,
    ) -> None:
        super().__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
        )
        self._rows.clear()  # Clear pre_fetch data
        self._arraysize = arraysize
        self._keep_default_na = keep_default_na
        self._na_values = na_values
        self._quoting = quoting
        self._unload = unload
        self._unload_location = unload_location
        self._engine = engine
        self._chunksize = chunksize
        self._block_size = block_size
        self._cache_type = cache_type
        self._max_workers = max_workers
        self._data_manifest: List[str] = []
        self._kwargs = kwargs
        self._fs = self.__s3_file_system()
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            df = self._as_pandas()
            trunc_date = _no_trunc_date if self.is_unload else self._trunc_date
            self._df_iter = DataFrameIterator(df, trunc_date)
        else:
            import pandas as pd

            self._df_iter = DataFrameIterator(pd.DataFrame(), _no_trunc_date)
        self._iterrows = self._df_iter.iterrows()

    def _get_parquet_engine(self) -> str:
        """Get the parquet engine to use, handling auto-detection.

        Returns:
            Name of the parquet engine to use ('pyarrow' or 'fastparquet').

        Raises:
            ImportError: If no suitable parquet engine is available.
        """
        if self._engine == "auto":
            return self._get_available_engine(["pyarrow", "fastparquet"])
        return self._engine

    def _get_csv_engine(self, file_size_bytes: Optional[int] = None) -> str:
        """Determine the appropriate CSV engine, similar to _get_parquet_engine pattern.

        Args:
            file_size_bytes: Size of the CSV file in bytes.

        Returns:
            CSV engine to use ('pyarrow', 'c', or 'python').
        """
        # Use self._engine if it's a valid CSV engine
        if self._engine in ("c", "python", "pyarrow"):
            if self._engine == "pyarrow":
                try:
                    self._get_available_engine(["pyarrow"])
                    return "pyarrow"
                except ImportError:
                    _logger.warning(
                        "PyArrow engine requested but not available, falling back to optimal engine"
                    )
                    # Use optimal fallback based on file size
                    return self._get_optimal_csv_engine(file_size_bytes)
            return self._engine

        # If self._engine is "auto" or a parquet engine, auto-determine for CSV
        if self._engine in ("auto", "fastparquet"):
            return self._get_optimal_csv_engine(file_size_bytes)

        # Fallback for unknown engine values
        _logger.warning(f"Unknown engine '{self._engine}', defaulting to optimal CSV engine")
        return self._get_optimal_csv_engine(file_size_bytes)

    def _get_available_engine(self, engine_candidates: List[str]) -> str:
        """Get the first available engine from a list of candidates.

        Args:
            engine_candidates: List of engine names to try in order.

        Returns:
            First available engine name.

        Raises:
            ImportError: If no engines are available.
        """
        import importlib

        error_msgs = ""
        for engine in engine_candidates:
            try:
                module = importlib.import_module(engine)
                return module.__name__
            except ImportError as e:
                error_msgs += f"\n - {str(e)}"

        available_engines = ", ".join(f"'{e}'" for e in engine_candidates)
        raise ImportError(
            f"Unable to find a usable engine; tried using: {available_engines}."
            f"Trying to import the above resulted in these errors:"
            f"{error_msgs}"
        )

    def _get_optimal_csv_engine(self, file_size_bytes: Optional[int] = None) -> str:
        """Get the optimal CSV engine based on availability and file size.

        Args:
            file_size_bytes: Size of the CSV file in bytes.

        Returns:
            CSV engine to use ('pyarrow', 'c', or 'python').
        """
        # PyArrow engine doesn't support chunksize, so avoid it when chunking is needed
        if self._chunksize is not None:
            # When chunking is required, choose between C and Python based on file size
            if file_size_bytes and file_size_bytes > 50 * 1024 * 1024:  # 50MB+
                # Use Python engine for large files to avoid C parser int32 limits
                return "python"
            # Use C engine for smaller files (better performance)
            return "c"

        # Try PyArrow first (best performance and memory efficiency) when not chunking
        try:
            self._get_available_engine(["pyarrow"])
            return "pyarrow"
        except ImportError:
            # PyArrow not available, choose between C and Python based on file size
            if file_size_bytes and file_size_bytes > 50 * 1024 * 1024:  # 50MB+
                # Use Python engine for large files to avoid C parser int32 limits
                return "python"
            # Use C engine for smaller files (better performance)
            return "c"

    def _auto_determine_chunksize(self, file_size_bytes: int) -> Optional[int]:
        """Automatically determine appropriate chunksize for large files to avoid memory issues.

        Args:
            file_size_bytes: Size of the result file in bytes.

        Returns:
            Suggested chunksize or None if chunking is not needed.
        """
        # For files larger than 50MB, consider chunking to avoid memory issues
        if file_size_bytes > 50 * 1024 * 1024:  # 50MB
            # Estimate rows: assume average 100 bytes per row (very rough)
            estimated_rows = file_size_bytes // 100
            if estimated_rows > 2_000_000:  # 2M+ rows
                # Suggest chunk size of 100K rows for very large datasets
                return 100_000
            if estimated_rows > 1_000_000:  # 1M+ rows
                # Suggest chunk size of 50K rows for large datasets
                return 50_000
        return None

    def __s3_file_system(self):
        from pyathena.filesystem.s3 import S3FileSystem

        return S3FileSystem(
            connection=self.connection,
            default_block_size=self._block_size,
            default_cache_type=self._cache_type,
            max_workers=self._max_workers,
        )

    @property
    def is_unload(self):
        return self._unload and self.query and self.query.strip().upper().startswith("UNLOAD")

    @property
    def dtypes(self) -> Dict[str, Type[Any]]:
        description = self.description if self.description else []
        return {
            d[0]: self._converter.types[d[1]] for d in description if d[1] in self._converter.types
        }

    @property
    def converters(
        self,
    ) -> Dict[Optional[Any], Callable[[Optional[str]], Optional[Any]]]:
        description = self.description if self.description else []
        return {
            d[0]: self._converter.get(d[1]) for d in description if d[1] in self._converter.mappings
        }

    @property
    def parse_dates(self) -> List[Optional[Any]]:
        description = self.description if self.description else []
        return [d[0] for d in description if d[1] in self._parse_dates]

    def _trunc_date(self, df: "DataFrame") -> "DataFrame":
        description = self.description if self.description else []
        times = [d[0] for d in description if d[1] in ("time", "time with time zone")]
        if times:
            truncated = df.loc[:, times].apply(lambda r: r.dt.time)
            for time in times:
                df.isetitem(df.columns.get_loc(time), truncated[time])
        return df

    def fetchone(
        self,
    ) -> Optional[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
        try:
            row = next(self._iterrows)
        except StopIteration:
            return None
        else:
            # Python int has no practical limit, but be safe with row indexing
            self._rownumber = row[0] + 1
            description = self.description if self.description else []
            return tuple([row[1][d[0]] for d in description])

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Union[Tuple[Optional[Any], ...], Dict[Any, Optional[Any]]]]:
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
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _read_csv(self) -> Union["TextFileReader", "DataFrame"]:
        import pandas as pd

        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        if not self.output_location.endswith((".csv", ".txt")):
            return pd.DataFrame()
        if self.substatement_type and self.substatement_type.upper() in (
            "UPDATE",
            "DELETE",
            "MERGE",
            "VACUUM_TABLE",
        ):
            return pd.DataFrame()
        length = self._get_content_length()
        if length and self.output_location.endswith(".txt"):
            sep = "\t"
            header = None
            description = self.description if self.description else []
            names = [d[0] for d in description]
        elif length and self.output_location.endswith(".csv"):
            sep = ","
            header = 0
            names = None
        else:
            return pd.DataFrame()

        # Auto-determine chunksize if not set and file is large
        effective_chunksize = self._chunksize
        use_auto_chunking = False
        original_chunksize = self._chunksize  # Store original value
        if effective_chunksize is None and length:
            auto_chunksize = self._auto_determine_chunksize(length)
            if auto_chunksize:
                effective_chunksize = auto_chunksize
                use_auto_chunking = True
                # Temporarily set _chunksize so _get_csv_engine can see it
                self._chunksize = auto_chunksize
                _logger.info(
                    f"Large file detected ({length} bytes). "
                    f"Automatically using chunksize={auto_chunksize} for better performance."
                )

        # Determine CSV engine using self._engine
        csv_engine = self._get_csv_engine(length)
        use_auto_engine = (
            csv_engine in ("pyarrow", "python")
            and self._engine in ("auto", "pyarrow", "fastparquet")
            and length
            and length > 10 * 1024 * 1024
        )

        # Prepare read_csv parameters with safeguards for large datasets
        read_csv_kwargs = {
            "sep": sep,
            "header": header,
            "names": names,
            "dtype": self.dtypes,
            "converters": self.converters,
            "parse_dates": self.parse_dates,
            "skip_blank_lines": False,
            "keep_default_na": self._keep_default_na,
            "na_values": self._na_values,
            "quoting": self._quoting,
            "storage_options": {
                "connection": self.connection,
                "default_block_size": self._block_size,
                "default_cache_type": self._cache_type,
                "max_workers": self._max_workers,
            },
            "chunksize": effective_chunksize,
            "engine": csv_engine,
        }

        # Set low_memory=False for Python engine with large files (non-chunked)
        # PyArrow engine doesn't support low_memory parameter
        if (
            csv_engine == "python"
            and effective_chunksize is None
            and length
            and length > 50 * 1024 * 1024
        ):
            read_csv_kwargs["low_memory"] = False

        # Log automatic engine selection
        if use_auto_engine and not use_auto_chunking:
            if csv_engine == "pyarrow":
                _logger.info(
                    "Using PyArrow CSV engine for large file (fastest and most memory efficient)."
                )
            elif csv_engine == "python":
                _logger.info(
                    "Using Python CSV engine for large file to avoid integer overflow issues."
                )

        # Apply user kwargs
        read_csv_kwargs.update(self._kwargs)

        try:
            return pd.read_csv(self.output_location, **read_csv_kwargs)
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            # Check for integer overflow related errors and suggest solutions
            error_msg = str(e).lower()
            if any(
                phrase in error_msg
                for phrase in ["signed integer", "maximum", "overflow", "int32", "c parser"]
            ):
                detailed_msg = (
                    f"Large dataset processing error: {e}. "
                    f"This is likely due to pandas C parser limitations with very large files. "
                    f"Try using chunksize or PyArrow engine: "
                    f"cursor = connection.cursor(PandasCursor, chunksize=100000, engine='pyarrow')"
                )
                raise OperationalError(detailed_msg) from e
            raise OperationalError(*e.args) from e
        finally:
            # Restore original chunksize value
            self._chunksize = original_chunksize

    def _read_parquet(self, engine) -> "DataFrame":
        import pandas as pd

        self._data_manifest = self._read_data_manifest()
        if not self._data_manifest:
            return pd.DataFrame()
        if not self._unload_location:
            self._unload_location = "/".join(self._data_manifest[0].split("/")[:-1]) + "/"

        if engine == "pyarrow":
            unload_location = self._unload_location
            kwargs = {
                "use_threads": True,
            }
        elif engine == "fastparquet":
            unload_location = f"{self._unload_location}*"
            kwargs = {}
        else:
            raise ProgrammingError("Engine must be one of `pyarrow`, `fastparquet`.")
        kwargs.update(self._kwargs)

        try:
            return pd.read_parquet(
                unload_location,
                engine=self._engine,
                storage_options={
                    "connection": self.connection,
                    "default_block_size": self._block_size,
                    "default_cache_type": self._cache_type,
                    "max_workers": self._max_workers,
                },
                **kwargs,
            )
        except Exception as e:
            _logger.exception(f"Failed to read {self.output_location}.")
            raise OperationalError(*e.args) from e

    def _read_parquet_schema(self, engine) -> Tuple[Dict[str, Any], ...]:
        if engine == "pyarrow":
            from pyarrow import parquet

            from pyathena.arrow.util import to_column_info

            if not self._unload_location:
                raise ProgrammingError("UnloadLocation is none or empty.")
            bucket, key = parse_output_location(self._unload_location)
            try:
                dataset = parquet.ParquetDataset(f"{bucket}/{key}", filesystem=self._fs)
                return to_column_info(dataset.schema)
            except Exception as e:
                _logger.exception(f"Failed to read schema {bucket}/{key}.")
                raise OperationalError(*e.args) from e
        elif engine == "fastparquet":
            from fastparquet import ParquetFile

            # TODO: https://github.com/python/mypy/issues/1153
            from pyathena.fastparquet.util import to_column_info  # type: ignore

            if not self._data_manifest:
                self._data_manifest = self._read_data_manifest()
            bucket, key = parse_output_location(self._data_manifest[0])
            try:
                file = ParquetFile(f"{bucket}/{key}", open_with=self._fs.open)
                return to_column_info(file.schema)
            except Exception as e:
                _logger.exception(f"Failed to read schema {bucket}/{key}.")
                raise OperationalError(*e.args) from e
        else:
            raise ProgrammingError("Engine must be one of `pyarrow`, `fastparquet`.")

    def _as_pandas(self) -> Union["TextFileReader", "DataFrame"]:
        if self.is_unload:
            engine = self._get_parquet_engine()
            df = self._read_parquet(engine)
            if df.empty:
                self._metadata = ()
            else:
                self._metadata = self._read_parquet_schema(engine)
        else:
            df = self._read_csv()
        return df

    def as_pandas(self) -> Union[DataFrameIterator, "DataFrame"]:
        if self._chunksize is None:
            return next(self._df_iter)
        return self._df_iter

    def close(self) -> None:
        import pandas as pd

        super().close()
        self._df_iter = DataFrameIterator(pd.DataFrame(), _no_trunc_date)
        self._iterrows = enumerate([])
        self._data_manifest = []
