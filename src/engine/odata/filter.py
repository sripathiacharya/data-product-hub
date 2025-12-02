from typing import Optional
import pandas as pd


def apply_odata_query(
    df: pd.DataFrame,
    select: Optional[str] = None,
    filter_expr: Optional[str] = None,
    top: Optional[int] = None,
    skip: Optional[int] = None,
    orderby: Optional[str] = None,
    entity=None,  # kept for future extension; currently unused
) -> pd.DataFrame:
    """
    Minimal OData-like query support:

      - $select: comma-separated list of columns
      - $top: limit rows
      - $skip: offset rows
      - $orderby: single column, `col` or `col desc`

    NOTE:
      - $filter is currently a no-op (ignored); we can add parsing later.
    """

    # ---- $orderby ----
    if orderby:
        # support forms: "col" or "col desc"
        parts = [p.strip() for p in orderby.split()]
        col = parts[0]
        ascending = True
        if len(parts) > 1 and parts[1].lower() == "desc":
            ascending = False

        if col in df.columns:
            df = df.sort_values(by=col, ascending=ascending)

    # ---- $skip ----
    if skip:
        df = df.iloc[skip:]

    # ---- $top ----
    if top:
        df = df.iloc[:top]

    # ---- $select ----
    if select:
        cols = [c.strip() for c in select.split(",") if c.strip()]
        existing = [c for c in cols if c in df.columns]
        if existing:
            df = df[existing]

    # ---- $filter (not yet implemented) ----
    _ = filter_expr  # currently unused; placeholder

    return df
