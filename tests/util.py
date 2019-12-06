# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import codecs
import contextlib
import functools


def with_cursor(work_group=None):
    def _with_cursor(fn):
        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            with contextlib.closing(self.connect(work_group=work_group)) as conn:
                with conn.cursor() as cursor:
                    fn(self, cursor, *args, **kwargs)
        return wrapped_fn
    return _with_cursor


def with_async_cursor():
    def _with_async_cursor(fn):
        from pyathena.async_cursor import AsyncCursor

        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            with contextlib.closing(self.connect()) as conn:
                with conn.cursor(AsyncCursor) as cursor:
                    fn(self, cursor, *args, **kwargs)
        return wrapped_fn
    return _with_async_cursor


def with_pandas_cursor():
    def _with_pandas_cursor(fn):
        from pyathena.pandas_cursor import PandasCursor

        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            with contextlib.closing(self.connect()) as conn:
                with conn.cursor(PandasCursor) as cursor:
                    fn(self, cursor, *args, **kwargs)
        return wrapped_fn
    return _with_pandas_cursor


def with_async_pandas_cursor():
    def _with_async_pandas_cursor(fn):
        from pyathena.async_pandas_cursor import AsyncPandasCursor

        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            with contextlib.closing(self.connect()) as conn:
                with conn.cursor(AsyncPandasCursor) as cursor:
                    fn(self, cursor, *args, **kwargs)
        return wrapped_fn
    return _with_async_pandas_cursor


def with_engine():
    def _with_engine(fn):
        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            engine = self.create_engine()
            try:
                with contextlib.closing(engine.connect()) as conn:
                    fn(self, engine, conn, *args, **kwargs)
            finally:
                engine.dispose()
        return wrapped_fn
    return _with_engine


def read_query(path):
    with codecs.open(path, 'rb', 'utf-8') as f:
        query = f.read()
    return [q.strip() for q in query.split(';') if q and q.strip()]
