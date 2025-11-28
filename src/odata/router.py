from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

from .registry import get_runtime, list_products
from .filter import apply_odata_filter, apply_odata_orderby

router = APIRouter(
    prefix="/odata",
    tags=["odata"],
)


@router.get("/$metadata")
def list_odata_products() -> List[dict]:
    """Simple metadata endpoint listing available OData entity sets."""
    return [
        {
            "id": cfg.id,
            "route": cfg.route,
            "description": cfg.description,
            "entity": cfg.entity.name,
        }
        for cfg in list_products()
    ]


@router.get("/{product_route}")
def query_product(
    product_route: str,
    select: Optional[str] = Query(None, alias="$select"),
    filter_: Optional[str] = Query(None, alias="$filter"),
    top: Optional[int] = Query(None, alias="$top", ge=1),
    skip: int = Query(0, alias="$skip", ge=0),
    orderby: Optional[str] = Query(None, alias="$orderby"),
) -> List[dict[str, Any]]:
    """
    Generic OData-style endpoint for any configured data product.

    Example:
      /odata/southafrica-scheduled-outage-dataset
      /odata/southafrica-scheduled-outage-dataset?$filter=province eq 'Gauteng'
    """
    try:
        runtime = get_runtime(product_route)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Data product '{product_route}' not found")

    cfg = runtime.config
    df = runtime.df.copy()

    # $filter
    if filter_:
        df = apply_odata_filter(df, filter_, cfg.odata.filterable)

    # $orderby
    if orderby:
        df = apply_odata_orderby(df, orderby, cfg.odata.orderable)

    # $select
    # always ensure key column is present so entities stay valid
    key_col = cfg.entity.key_column
    all_cols = [c.name for c in cfg.entity.columns]
    if select:
        requested = [c.strip() for c in select.split(",") if c.strip()]
        columns = [key_col] + [c for c in requested if c in all_cols and c != key_col]
    else:
        columns = all_cols

    df = df[columns]

    # $top / $skip
    max_top = cfg.odata.max_top
    effective_top = top or cfg.odata.default_top
    effective_top = min(effective_top, max_top)
    df = df.iloc[skip : skip + effective_top]

    # Return as list of dicts
    return df.to_dict(orient="records")
