# -*- coding: utf-8 -*-


class HashableDict(dict):  # type: ignore
    def __hash__(self):
        return hash(tuple(sorted(self.items())))
