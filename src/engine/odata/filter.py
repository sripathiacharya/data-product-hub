# src/engine/odata/filter.py

from typing import Optional, Tuple, List
import re
import logging

logger = logging.getLogger(__name__)


def _translate_odata_to_sql(filter_expr: str) -> str:
    """
    Translate a subset of OData $filter syntax into a SQL expression
    suitable for DuckDB.

    Supported:
      - Comparison: eq, ne, gt, ge, lt, le
      - Logical: and, or
      - Literals: true/false/null
      - Parentheses are passed through as-is.

    NOTE:
      * We assume identifiers are valid column names.
      * We do not yet support functions (startswith, substring, etc.).
      * This is intended for internal usage, not arbitrary external user input.
    """
    expr = filter_expr

    # Map OData keywords to SQL equivalents using word-boundary regexes
    replacements = {
        r"\beq\b": "=",
        r"\bne\b": "<>",
        r"\bgt\b": ">",
        r"\bge\b": ">=",
        r"\blt\b": "<",
        r"\ble\b": "<=",
        r"\band\b": "AND",
        r"\bor\b": "OR",
        r"\btrue\b": "TRUE",
        r"\bfalse\b": "FALSE",
        r"\bnull\b": "NULL",
    }

    for pattern, repl in replacements.items():
        expr = re.sub(pattern, f" {repl} ", expr, flags=re.IGNORECASE)

    # Normalize whitespace a bit
    expr = re.sub(r"\s+", " ", expr).strip()
    return expr


def build_where_clause(filter_expr: Optional[str]) -> Tuple[Optional[str], List[object]]:
    """
    Convert an OData-style $filter expression into a SQL WHERE clause fragment.

    Returns:
      (sql_fragment, params)

    For now, we inline all literals directly into the generated SQL fragment
    and return an empty params list. In the future, this can be extended to
    produce parameterized queries instead.

    If the filter expression cannot be translated, we log a warning and return
    (None, []) so that the caller can ignore the filter rather than failing.
    """
    if not filter_expr:
        return None, []

    try:
        sql = _translate_odata_to_sql(filter_expr)
        logger.info("Translated $filter to SQL: %s -> %s", filter_expr, sql)
        return sql, []
    except Exception as e:
        logger.warning(
            "Failed to translate $filter to SQL. original=%r error=%r",
            filter_expr,
            e,
        )
        return None, []
