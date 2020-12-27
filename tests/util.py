# -*- coding: utf-8 -*-
import codecs
import contextlib
import functools


def with_cursor(**opts):
    def _with_cursor(fn):
        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            with contextlib.closing(self.connect(**opts)) as conn:
                with conn.cursor() as cursor:
                    fn(self, cursor, *args, **kwargs)

        return wrapped_fn

    return _with_cursor


def with_engine(**opts):
    def _with_engine(fn):
        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            engine = self.create_engine(**opts)
            try:
                with contextlib.closing(engine.connect()) as conn:
                    fn(self, engine, conn, *args, **kwargs)
            finally:
                engine.dispose()

        return wrapped_fn

    return _with_engine


def read_query(path):
    with codecs.open(path, "rb", "utf-8") as f:
        query = f.read()
    return [q.strip() for q in query.split(";") if q and q.strip()]
