# -*- coding: utf-8 -*-


class _HashableDict(dict):  # type: ignore
    def __hash__(self):  # type: ignore
        return hash(tuple(sorted(self.items())))
