# -*- coding: utf-8 -*-
import codecs


def read_query(path):
    with codecs.open(path, "rb", "utf-8") as f:
        query = f.read()
    return [q.strip() for q in query.split(";") if q and q.strip()]
