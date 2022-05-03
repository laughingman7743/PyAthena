# -*- coding: utf-8 -*-
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_queries = Environment(
    loader=FileSystemLoader(Path(__file__).parent.resolve() / "resources" / "queries")
)


def read_query(name, **kwargs):
    template = _queries.get_template(name)
    return [q.strip() for q in template.render(**kwargs).split(";") if q and q.strip()]
