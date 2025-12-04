# src/engine/odata/filter.py

from typing import Optional
import re
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _translate_odata_filter(filter_expr: str) -> str:
    """
    Translate a subset of OData $filter syntax into a pandas.query expression.

    Supported:
      - Comparison: eq, ne, gt, ge, lt, le
      - Logical: and, or
      - Literals: true/false/null
      - Parentheses are passed through as-is.

    Example:
      "province eq 'Gauteng' and stage ge 3"
      --> "province == 'Gauteng' & stage >= 3"
    """
    expr = filter_expr

    # Map OData keywords to pandas/query / Python equivalents using word-boundary regexes
    replacements = {
        r"\beq\b": "==",
        r"\bne\b": "!=",
        r"\bgt\b": ">",
        r"\bge\b": ">=",
        r"\blt\b": "<",
        r"\ble\b": "<=",
        r"\band\b": "&",
        r"\bor\b": "|",
        r"\btrue\b": "True",
        r"\bfalse\b": "False",
        r"\bnull\b": "None",
    }

    for pattern, repl in replacements.items():
        expr = re.sub(pattern, f" {repl} ", expr, flags=re.IGNORECASE)

    # Normalize whitespace a bit
    expr = re.sub(r"\s+", " ", expr).strip()
    return expr


def _apply_filter(df: pd.DataFrame, filter_expr: Optional[str], entity=None) -> pd.DataFrame:
    """
    Apply OData-like $filter to a DataFrame.

    For now we:
      - trust the filter expression (internal API),
      - translate OData operators,
      - evaluate with df.query(engine="python").

    If evaluation fails for any reason, we fall back to returning the original df
    but log the error so you can debug.
    """
    if not filter_expr:
        return df

    logger.info("Applying $filter: %s", filter_expr)
    translated = _translate_odata_filter(filter_expr)
    logger.info("Translated $filter for pandas.query: %s", translated)

    # TODO (hardening): validate column names against df.columns or entity config.
    try:
        return df.query(translated, engine="python")
    except Exception as e:
        logger.warning(
            "Failed to apply $filter. original=%r translated=%r error=%r",
            filter_expr,
            translated,
            e,
        )
        # Graceful degradation: ignore invalid filters instead of 500.
        # If you prefer strictness, raise and map to HTTP 400.
        return df


def apply_odata_query(
    df: pd.DataFrame,
    select: Optional[str] = None,
    filter_expr: Optional[str] = None,
    top: Optional[int] = None,
    skip: Optional[int] = None,
    orderby: Optional[str] = None,
    entity=None,  # reserved for future use (e.g. validation against entity metadata)
) -> pd.DataFrame:
    """
    Minimal OData-like query support:

      - $filter: basic expressions with eq/ne/gt/ge/lt/le, and/or, parentheses.
      - $orderby: single column, `col` or `col desc`
      - $skip: offset rows
      - $top: limit rows
      - $select: comma-separated list of columns

    The order of operations is:
      1. filter
      2. orderby
      3. skip
      4. top
      5. select
    """

    # ---- $filter ----
    if filter_expr:
        df = _apply_filter(df, filter_expr, entity)

    # ---- $orderby ----
    if orderby:
        parts = [p.strip() for p in orderby.split()]
        col = parts[0]
        ascending = True
        if len(parts) > 1 and parts[1].lower() == "desc":
            ascending = False

        if col in df.columns:
            logger.info("Applying $orderby on column=%s ascending=%s", col, ascending)
            df = df.sort_values(by=col, ascending=ascending)
        else:
            logger.warning("Ignoring $orderby: column %r not in df.columns", col)

    # ---- $skip ----
    if skip:
        logger.info("Applying $skip=%s", skip)
        df = df.iloc[skip:]

    # ---- $top ----
    if top:
        logger.info("Applying $top=%s", top)
        df = df.iloc[:top]

    # ---- $select ----
    if select:
        cols = [c.strip() for c in select.split(",") if c.strip()]
        existing = [c for c in cols if c in df.columns]
        logger.info("Applying $select=%s -> existing columns=%s", cols, existing)
        if existing:
            df = df[existing]

    return df
