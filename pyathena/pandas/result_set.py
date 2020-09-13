# -*- coding: utf-8 -*-
import logging

from pyathena.error import OperationalError, ProgrammingError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import parse_output_location, retry_api_call

_logger = logging.getLogger(__name__)


class AthenaPandasResultSet(AthenaResultSet):

    _parse_dates = [
        "date",
        "time",
        "time with time zone",
        "timestamp",
        "timestamp with time zone",
    ]

    def __init__(
        self,
        connection,
        converter,
        query_execution,
        arraysize,
        retry_config,
        keep_default_na=False,
        na_values=None,
        quoting=1,
    ):
        super(AthenaPandasResultSet, self).__init__(
            connection=connection,
            converter=converter,
            query_execution=query_execution,
            arraysize=1,  # Fetch one row to retrieve metadata
            retry_config=retry_config,
        )
        self._arraysize = arraysize
        self._keep_default_na = keep_default_na
        self._na_values = na_values
        self._quoting = quoting
        self._client = self._connection.session.client(
            "s3",
            region_name=self._connection.region_name,
            **self._connection._client_kwargs
        )
        if (
            self.state == AthenaQueryExecution.STATE_SUCCEEDED
            and self.output_location.endswith((".csv", ".txt"))
        ):
            self._df = self._as_pandas()
        else:
            import pandas as pd

            self._df = pd.DataFrame()
        self._iterrows = self._df.iterrows()

    @property
    def dtypes(self):
        return {
            d[0]: self._converter.types[d[1]]
            for d in self.description
            if d[1] in self._converter.types
        }

    @property
    def converters(self):
        return {
            d[0]: self._converter.mappings[d[1]]
            for d in self.description
            if d[1] in self._converter.mappings
        }

    @property
    def parse_dates(self):
        return [d[0] for d in self.description if d[1] in self._parse_dates]

    def _trunc_date(self, df):
        times = [
            d[0] for d in self.description if d[1] in ("time", "time with time zone")
        ]
        if times:
            df.loc[:, times] = df.loc[:, times].apply(lambda r: r.dt.time)
        return df

    def _fetch(self):
        try:
            row = next(self._iterrows)
        except StopIteration:
            return None
        else:
            self._rownumber = row[0] + 1
            return tuple([row[1][d[0]] for d in self.description])

    def fetchone(self):
        return self._fetch()

    def fetchmany(self, size=None):
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

    def _as_pandas(self):
        import pandas as pd

        if not self.output_location:
            raise ProgrammingError("OutputLocation is none or empty.")
        bucket, key = parse_output_location(self.output_location)
        try:
            response = retry_api_call(
                self._client.get_object,
                config=self._retry_config,
                logger=_logger,
                Bucket=bucket,
                Key=key,
            )
        except Exception as e:
            _logger.exception("Failed to download csv.")
            raise OperationalError(*e.args) from e
        else:
            length = response["ContentLength"]
            if length:
                if self.output_location.endswith(".txt"):
                    sep = "\t"
                    header = None
                    names = [d[0] for d in self.description]
                else:  # csv format
                    sep = ","
                    header = 0
                    names = None
                df = pd.read_csv(
                    response["Body"],
                    sep=sep,
                    header=header,
                    names=names,
                    dtype=self.dtypes,
                    converters=self.converters,
                    parse_dates=self.parse_dates,
                    infer_datetime_format=True,
                    skip_blank_lines=False,
                    keep_default_na=self._keep_default_na,
                    na_values=self._na_values,
                    quoting=self._quoting,
                )
                df = self._trunc_date(df)
            else:  # Allow empty response
                df = pd.DataFrame()
            return df

    def as_pandas(self):
        return self._df

    def close(self):
        super(AthenaPandasResultSet, self).close()
        self._df = None
        self._iterrows = None
