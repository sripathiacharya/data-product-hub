from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Depends
from urllib.parse import urlencode
import logging

from .registry import list_products, get_runtime
from .filter import apply_odata_query

from ..security.dependency import get_current_principal
from ..security.authorization import check_dataset_access

logger = logging.getLogger(__name__)


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
    principal: Optional[dict] = Depends(get_current_principal),
):
    """
    Query the main (joined) dataset for a product.
    """
    runtime = get_runtime(product_route)
    if runtime is None:
        raise HTTPException(status_code=404, detail=f"Unknown data product '{product_route}'")

    check_dataset_access(runtime, principal)

    df = runtime.df

    logger.info(
        "Query product=%s $filter=%r $select=%r $top=%r $skip=%r $orderby=%r",
        product_route,
        filter_,
        select,
        top,
        skip,
        orderby,
    )

    # --- total count AFTER filter, BEFORE paging ---
    df_filtered_for_count = apply_odata_query(
        df=df,
        select=None,
        filter_expr=filter_,
        top=None,
        skip=None,
        orderby=None,
        entity=runtime.config.entity,
    )
    total_count = len(df_filtered_for_count)
    logger.info("Filtered total_count=%s for product=%s", total_count, product_route)
    print(f"Filtered total_count={total_count} for product={product_route}")

    # Apply default_top / max_top policy
    eff_top = _effective_top(top, runtime)

    # --- page data (filter + orderby + skip + top + select) ---
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

    # Build @odata.nextLink if there is another page AFTER filtering
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
    principal: Optional[dict] = Depends(get_current_principal),
):
    """
    Query a raw backend source (e.g. 'areas', 'schedule') independently.
    """
    runtime = get_runtime(product_route)
    if runtime is None:
        raise HTTPException(status_code=404, detail=f"Unknown data product '{product_route}'")

    check_dataset_access(runtime, principal)

    if source_name not in runtime.raw:
        raise HTTPException(
            status_code=404,
            detail=f"Data source '{source_name}' not found for product '{product_route}'",
        )

    df = runtime.raw[source_name]

    logger.info(
        "Query source product=%s source=%s $filter=%r $select=%r $top=%r $skip=%r $orderby=%r",
        product_route,
        source_name,
        filter_,
        select,
        top,
        skip,
        orderby,
    )

    # --- total count AFTER filter, BEFORE paging ---
    df_filtered_for_count = apply_odata_query(
        df=df,
        select=None,
        filter_expr=filter_,
        top=None,
        skip=None,
        orderby=None,
        entity=None,
    )
    total_count = len(df_filtered_for_count)
    logger.info(
        "Filtered total_count=%s for product=%s source=%s",
        total_count,
        product_route,
        source_name,
    )

    eff_top = _effective_top(top, runtime)

    df_page = apply_odata_query(
        df=df,
        select=select,
        filter_expr=filter_,
        top=eff_top,
        skip=skip,
        orderby=orderby,
        entity=None,
    )

    records = df_page.to_dict(orient="records")

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
