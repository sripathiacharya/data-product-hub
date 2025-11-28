import re
import pandas as pd


def apply_odata_filter(df: pd.DataFrame, filter_expr: str, allowed_cols: list[str]) -> pd.DataFrame:
    """
    Very small OData subset:
      col eq 'value' or col eq 123
    Combined with 'and'.
    Example:
      province eq 'Gauteng' and city eq 'Johannesburg' and suburb eq 'Bromhof'
    """
    exprs = [e.strip() for e in filter_expr.split(" and ") if e.strip()]
    mask = pd.Series(True, index=df.index)

    for e in exprs:
        m = re.match(r"(?P<col>\w+)\s+eq\s+(?P<val>.+)", e)
        if not m:
            # ignore unsupported expressions
            continue

        col = m.group("col")
        if col not in allowed_cols:
            # block filtering on non-exposed columns
            continue

        raw_val = m.group("val").strip()

        # strip single quotes if present
        if raw_val.startswith("'") and raw_val.endswith("'"):
            val = raw_val[1:-1]
        else:
            # try numeric
            try:
                val = int(raw_val)
            except ValueError:
                try:
                    val = float(raw_val)
                except ValueError:
                    val = raw_val

        if col in df.columns:
            mask &= df[col] == val
        elif col.lower() in df.columns:
            mask &= df[col.lower()] == val

    return df[mask]


def apply_odata_orderby(df: pd.DataFrame, orderby: str, allowed_cols: list[str]) -> pd.DataFrame:
    """
    Supports: col or col desc
    """
    parts = orderby.split()
    col = parts[0]
    direction = parts[1].lower() if len(parts) > 1 else "asc"
    ascending = direction != "desc"

    if col not in allowed_cols:
        return df

    if col in df.columns:
        return df.sort_values(col, ascending=ascending)
    if col.lower() in df.columns:
        return df.sort_values(col.lower(), ascending=ascending)
    return df
