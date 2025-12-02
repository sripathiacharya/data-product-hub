from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from urllib.parse import urlencode

from .registry import list_products, get_runtime


# ------------------------------------------------------------------
# Minimal OData-like query support (local helper)
# ------------------------------------------------------------------
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

    _ = filter_expr  # placeholder for future $filter implementation

    return df


# ------------------------------------------------------------------
# Helpers for OData-style paging / limits
# ------------------------------------------------------------------
def _effective_top(requested_top: Optional[int], runtime) -> Optional[int]:
    """
    Apply default_top and max_top from config.odata if present.
    """
    odata_cfg = getattr(runtime.config, "odata", None)

    top = requested_top

    # default_top if nothing requested
    if top is None and odata_cfg is not None:
        default_top = getattr(odata_cfg, "default_top", None)
        if default_top is not None:
            top = default_top

    # clamp to max_top
    if odata_cfg is not None and top is not None:
        max_top = getattr(odata_cfg, "max_top", None)
        if max_top is not None and top > max_top:
            top = max_top

    return top


def _build_next_link_base(
    base_path: str,
    select: Optional[str],
    filter_: Optional[str],
    orderby: Optional[str],
    next_skip: int,
    top: int,
) -> str:
    """
    Build an OData-style @odata.nextLink URL with the same query params plus new $skip/$top.
    """
    params = {
        "$skip": next_skip,
        "$top": top,
    }
    if select:
        params["$select"] = select
    if filter_:
        params["$filter"] = filter_
    if orderby:
        params["$orderby"] = orderby

    return f"{base_path}?{urlencode(params)}"


# ------------------------------------------------------------------
# Router + endpoints
# ------------------------------------------------------------------
router = APIRouter(prefix="/odata", tags=["odata"])


@router.get("/$metadata")
def get_metadata():
    """
    List all configured data products and their basic metadata.
    NOTE: This is NOT full OData CSDL metadata, just a lightweight view.
    """
    products = []
    for runtime in list_products():
        cfg = runtime.config
        products.append(
            {
                "id": cfg.id,
                "route": cfg.route,
                "description": cfg.description,
                "entity": cfg.entity.name,
            }
        )
    return products


@router.get("/{product_route}")
def query_product(
    product_route: str,
    # OData-like query params
    select: Optional[str] = Query(default=None, alias="$select"),
    filter_: Optional[str] = Query(default=None, alias="$filter"),
    top: Optional[int] = Query(default=None, alias="$top"),
    skip: Optional[int] = Query(default=None, alias="$skip"),
    orderby: Optional[str] = Query(default=None, alias="$orderby"),
):
    """
    Query the main (joined) dataset for a product.

    Example:
      /odata/southafrica-scheduled-outage-dataset?$top=10
    """
    runtime = get_runtime(product_route)
    if runtime is None:
        raise HTTPException(status_code=404, detail=f"Unknown data product '{product_route}'")

    df = runtime.df

    # Total count BEFORE pagination (since we don't implement $filter yet,
    # this is simply the total number of rows)
    total_count = len(df)

    # Apply default_top / max_top policy
    eff_top = _effective_top(top, runtime)

    # Apply query
    df_page = apply_odata_query(
        df=df,
        select=select,
        filter_expr=filter_,
        top=eff_top,
        skip=skip,
        orderby=orderby,
        entity=runtime.config.entity,
    )

    records = df_page.to_dict(orient="records")

    # Build @odata.nextLink if there is another page
    next_link = None
    if eff_top is not None:
        current_skip = skip or 0
        next_skip = current_skip + eff_top
        if next_skip < total_count:
            base_path = f"/odata/{product_route}"
            next_link = _build_next_link_base(
                base_path=base_path,
                select=select,
                filter_=filter_,
                orderby=orderby,
                next_skip=next_skip,
                top=eff_top,
            )

    response = {
        "@odata.context": f"/odata/$metadata#{product_route}",
        "@odata.count": total_count,
        "value": records,
    }
    if next_link:
        response["@odata.nextLink"] = next_link

    return response


@router.get("/{product_route}/{source_name}")
def query_product_source(
    product_route: str,
    source_name: str,
    # Same OData-like query params
    select: Optional[str] = Query(default=None, alias="$select"),
    filter_: Optional[str] = Query(default=None, alias="$filter"),
    top: Optional[int] = Query(default=None, alias="$top"),
    skip: Optional[int] = Query(default=None, alias="$skip"),
    orderby: Optional[str] = Query(default=None, alias="$orderby"),
):
    """
    Query a raw backend source (e.g. 'areas', 'schedule') independently.

    Examples:
      /odata/southafrica-scheduled-outage-dataset/areas?$top=5
      /odata/southafrica-scheduled-outage-dataset/schedule?$top=5
    """
    runtime = get_runtime(product_route)
    if runtime is None:
        raise HTTPException(status_code=404, detail=f"Unknown data product '{product_route}'")

    if source_name not in runtime.raw:
        raise HTTPException(
            status_code=404,
            detail=f"Data source '{source_name}' not found for product '{product_route}'",
        )

    df = runtime.raw[source_name]

    # Total count BEFORE pagination
    total_count = len(df)

    # Apply default_top / max_top policy (same as joined)
    eff_top = _effective_top(top, runtime)

    # Apply query
    df_page = apply_odata_query(
        df=df,
        select=select,
        filter_expr=filter_,
        top=eff_top,
        skip=skip,
        orderby=orderby,
        entity=None,  # no per-source metadata yet
    )

    records = df_page.to_dict(orient="records")

    # Build @odata.nextLink if there is another page
    next_link = None
    if eff_top is not None:
        current_skip = skip or 0
        next_skip = current_skip + eff_top
        if next_skip < total_count:
            base_path = f"/odata/{product_route}/{source_name}"
            next_link = _build_next_link_base(
                base_path=base_path,
                select=select,
                filter_=filter_,
                orderby=orderby,
                next_skip=next_skip,
                top=eff_top,
            )

    response = {
        "@odata.context": f"/odata/$metadata#{product_route}/{source_name}",
        "@odata.count": total_count,
        "value": records,
    }
    if next_link:
        response["@odata.nextLink"] = next_link

    return response
