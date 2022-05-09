# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, cast

from pyathena.converter import Converter
from pyathena.error import ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig, parse_output_location

if TYPE_CHECKING:
    from pyarrow import Table

    from pyathena.connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class AthenaArrowResultSet(AthenaResultSet):

    DEFAULT_BLOCK_SIZE = 1024 * 1024 * 1024

    _timestamp_parsers: List[str] = [
        # TODO pyarrow.lib.ArrowInvalid:
        # In CSV column #1: CSV conversion error to timestamp[ms]:
        # invalid value '2022-01-01 11:22:33.123 UTC'
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f %Z",
        "%Y-%m-%d %H:%M:%S.%f %z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f %Z",
        "%Y-%m-%dT%H:%M:%S.%f %z",
    ]

    def __init__(
        self,
        connection: "Connection",
        converter: Converter,
        query_execution: AthenaQueryExecution,
        arraysize: int,
        retry_config: RetryConfig,
        block_size: Optional[int] = None,
        **kwargs,
    ) -> None:
        super(AthenaArrowResultSet, self).__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
        )
        self._arraysize = arraysize
        self._block_size = block_size if block_size else self.DEFAULT_BLOCK_SIZE
        self._kwargs = kwargs
        self._fs = self.__s3_file_system()
        if self.state == AthenaQueryExecution.STATE_SUCCEEDED and self.output_location:
            self._table = self._as_arrow()
        else:
            import pyarrow as pa

            self._table = pa.Table.from_pydict(dict())
        self._batches = iter(self._table.to_batches(arraysize))

    def __s3_file_system(self):
        from pyarrow import fs

        connection = cast("Connection", self._connection)
        if "role_arn" in connection._kwargs and connection._kwargs["role_arn"]:
            fs = fs.S3FileSystem(
                role_arn=connection._kwargs.get("role_arn", None),
                session_name=connection._kwargs.get("role_session_name", None),
                load_frequency=connection._kwargs.get("duration_seconds", None),
                region=connection.region_name,
            )
        else:
            fs = fs.S3FileSystem(
                access_key=connection._kwargs.get("aws_access_key_id", None),
                secret_key=connection._kwargs.get("aws_secret_access_key", None),
                session_token=connection._kwargs.get("aws_session_token", None),
                region=connection.region_name,
            )
        return fs

    @property
    def converters(
        self,
    ) -> Dict[str, Callable[[Optional[str]], Optional[Any]]]:
        description = self.description if self.description else []
        return {d[0]: self._converter.get(d[1]) for d in description}

    @property
    def column_types(self) -> Dict[str, Type[Any]]:
        import pyarrow as pa

        description = self.description if self.description else []
        types = self._converter.types
        return {
            d[0]: types.get(d[1], pa.string()) for d in description if d[1] in types
        }

    def _fetch(self) -> None:
        try:
            rows = next(self._batches)
        except StopIteration:
            return
        else:
            dict_rows = rows.to_pydict()
            column_names = dict_rows.keys()
            processed_rows = [
                tuple(self.converters[k](v) for k, v in zip(column_names, row))
                for row in zip(*dict_rows.values())
            ]
            self._rows.extend(processed_rows)

    def fetchone(self):
        if not self._rows:
            self._fetch()
        if not self._rows:
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        return self._rows.popleft()

    def fetchmany(self, size: Optional[int] = None):
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

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def _read_csv(self) -> "Table":
        import pyarrow as pa
        from pyarrow import csv

        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        if not self.output_location.endswith((".csv", ".txt")):
            return pa.Table.from_pydict(dict())
        bucket, key = parse_output_location(self.output_location)
        return csv.read_csv(
            self._fs.open_input_file(f"{bucket}/{key}"),
            read_options=csv.ReadOptions(skip_rows=0, block_size=self._block_size),
            parse_options=csv.ParseOptions(
                delimiter=",",
                quote_char='"',
                double_quote=True,
                escape_char=False,
            ),
            convert_options=csv.ConvertOptions(
                quoted_strings_can_be_null=False,
                timestamp_parsers=self._timestamp_parsers,
                column_types=self.column_types,
            ),
        )

    def _as_arrow(self) -> "Table":
        # TODO unload read parquet
        # if (
        #     self._unload
        #     and self.query
        #     and self.query.strip().upper().startswith("UNLOAD")
        # ):
        #     table = self._read_parquet()
        # else:
        #     table = self._read_csv()
        # return table
        return self._read_csv()

    def as_arrow(self) -> "Table":
        return self._table

    def close(self) -> None:
        import pyarrow as pa

        super(AthenaArrowResultSet, self).close()
        self._table = pa.Table.from_pydict(dict())
        self._batches = []
