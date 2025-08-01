# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Set

from sqlalchemy.sql.compiler import ILLEGAL_INITIAL_CHARACTERS, IdentifierPreparer

from pyathena.sqlalchemy.constants import DDL_RESERVED_WORDS, SELECT_STATEMENT_RESERVED_WORDS

if TYPE_CHECKING:
    from sqlalchemy import Dialect


class AthenaDMLIdentifierPreparer(IdentifierPreparer):
    reserved_words: Set[str] = SELECT_STATEMENT_RESERVED_WORDS


class AthenaDDLIdentifierPreparer(IdentifierPreparer):
    reserved_words = DDL_RESERVED_WORDS
    illegal_initial_characters = ILLEGAL_INITIAL_CHARACTERS.union("_")

    def __init__(
        self,
        dialect: "Dialect",
        initial_quote: str = "`",
        final_quote: Optional[str] = None,
        escape_quote: str = "`",
        quote_case_sensitive_collations: bool = True,
        omit_schema: bool = False,
    ):
        super().__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )
