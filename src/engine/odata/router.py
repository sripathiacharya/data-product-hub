# src/engine/odata/router.py

from typing import Optional, List, Tuple
import json
import logging
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse

from .registry import (
    list_products,
    get_runtime,
    get_duckdb_connection,
)
from .filter import build_where_clause

from ..security.dependency import get_current_principal
from ..security.authorization import check_dataset_access

logger = logging.getLogger(__name__)

# Obtain shared DuckDB connection & lock
_DUCKDB_CONN, _DUCKDB_LOCK = get_duckdb_connection()


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


def _build_select_list(select: Optional[str]) -> str:
    if not select:
        return "*"
    cols = [c.strip() for c in select.split(",") if c.strip()]
    if not cols:
        return "*"
    return ", ".join(f'"{c}"' for c in cols)


def _build_order_by(orderby: Optional[str]) -> str:
    if not orderby:
        return ""
    # For now, expect clauses like "col" or "col desc"
    items = [i.strip() for i in orderby.split(",") if i.strip()]
    if not items:
        return ""
    # TODO: add identifier quoting if needed
    return " ORDER BY " + ", ".join(items)


def _build_sql_for_query(
    base_view: str,
    select: Optional[str],
    filter_: Optional[str],
    orderby: Optional[str],
    top: Optional[int],
    skip: Optional[int],
) -> Tuple[str, List[object]]:
    select_clause = _build_select_list(select)
    where_clause, params = build_where_clause(filter_)
    order_clause = _build_order_by(orderby)

    limit_clause = ""
    if top is not None:
        limit_clause += f" LIMIT {int(top)}"
    if skip is not None:
        limit_clause += f" OFFSET {int(skip)}"

    sql = f'SELECT {select_clause} FROM "{base_view}"'
    if where_clause:
        sql += f" WHERE {where_clause}"
    sql += order_clause + limit_clause

    return sql, params


def _build_sql_for_count(base_view: str, filter_: Optional[str]) -> Tuple[str, List[object]]:
    where_clause, params = build_where_clause(filter_)
    sql = f'SELECT COUNT(*) AS cnt FROM "{base_view}"'
    if where_clause:
        sql += f" WHERE {where_clause}"
    return sql, params


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
                "route": cfg.route_key,
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
    count: Optional[bool] = Query(default=None, alias="$count"),
    stream: Optional[bool] = Query(default=False, alias="$stream"),
    principal: Optional[dict] = Depends(get_current_principal),
):
    """
    Query the main (joined) dataset for a product.

    If $stream=true, results are returned as a StreamingResponse where the JSON
    payload is written incrementally.
    """
    runtime = get_runtime(product_route)
    if runtime is None:
        raise HTTPException(status_code=404, detail=f"Unknown data product '{product_route}'")

    check_dataset_access(runtime, principal)

    logger.info(
        "Query product=%s $filter=%r $select=%r $top=%r $skip=%r $orderby=%r $stream=%r",
        product_route,
        filter_,
        select,
        top,
        skip,
        orderby,
        stream,
    )

    base_view = runtime.joined_view

    # Compute OData paging limits
    eff_top = _effective_top(top, runtime)

    # --- total count AFTER filter, BEFORE paging (if requested) ---
    total_count = None
    if count or count is None:
        # OData: $count=true or absence sometimes implies count; here we opt-in if $count=true
        if count:
            count_sql, count_params = _build_sql_for_count(base_view, filter_)
            with _DUCKDB_LOCK:
                cur = _DUCKDB_CONN.execute(count_sql, count_params)
                total_count = cur.fetchone()[0]
                logger.info("Filtered total_count=%s for product=%s", total_count, product_route)

    # Build main query SQL
    sql, params = _build_sql_for_query(
        base_view=base_view,
        select=select,
        filter_=filter_,
        orderby=orderby,
        top=eff_top,
        skip=skip,
    )

    # Pre-compute nextLink if we have a count and a top
    next_link = None
    if total_count is not None and eff_top is not None:
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

    # ---------- Non-streaming path ----------
    if not stream:
        with _DUCKDB_LOCK:
            cur = _DUCKDB_CONN.execute(sql, params)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]

        def row_to_obj(row):
            return {col: val for col, val in zip(columns, row)}

        body = {
            "@odata.context": f"/odata/$metadata#{product_route}",
            "value": [row_to_obj(r) for r in rows],
        }
        if total_count is not None:
            body["@odata.count"] = total_count
        if next_link:
            body["@odata.nextLink"] = next_link

        return body

    # ---------- Streaming path ----------
    def row_iterator():
        meta = {"@odata.context": f"/odata/$metadata#{product_route}"}
        if total_count is not None:
            meta["@odata.count"] = total_count
        if next_link:
            meta["@odata.nextLink"] = next_link

        # Start JSON object and "value" array
        head = json.dumps(meta, separators=(",", ":"))[:-1]  # strip closing '}'
        yield head
        yield ',"value":['

        first = True
        with _DUCKDB_LOCK:
            cur = _DUCKDB_CONN.execute(sql, params)
            columns = [d[0] for d in cur.description]

            while True:
                chunk = cur.fetchmany(1000)
                if not chunk:
                    break
                for row in chunk:
                    if not first:
                        yield ","
                    else:
                        first = False
                    obj = {col: val for col, val in zip(columns, row)}
                    yield json.dumps(obj, default=str, separators=(",", ":"))

        # Close array and object
        yield "]}"

    return StreamingResponse(row_iterator(), media_type="application/json")


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
    count: Optional[bool] = Query(default=None, alias="$count"),
    stream: Optional[bool] = Query(default=False, alias="$stream"),
    principal: Optional[dict] = Depends(get_current_principal),
):
    """
    Query a raw backend source (e.g. 'areas', 'schedule') independently.
    """
    runtime = get_runtime(product_route)
    if runtime is None:
        raise HTTPException(status_code=404, detail=f"Unknown data product '{product_route}'")

    check_dataset_access(runtime, principal)

    if source_name not in runtime.source_views:
        raise HTTPException(
            status_code=404,
            detail=f"Data source '{source_name}' not found for product '{product_route}'",
        )

    logger.info(
        "Query source product=%s source=%s $filter=%r $select=%r $top=%r $skip=%r $orderby=%r $stream=%r",
        product_route,
        source_name,
        filter_,
        select,
        top,
        skip,
        orderby,
        stream,
    )

    base_view = runtime.source_views[source_name]
    eff_top = _effective_top(top, runtime)

    total_count = None
    if count:
        count_sql, count_params = _build_sql_for_count(base_view, filter_)
        with _DUCKDB_LOCK:
            cur = _DUCKDB_CONN.execute(count_sql, count_params)
            total_count = cur.fetchone()[0]
            logger.info(
                "Filtered total_count=%s for product=%s source=%s",
                total_count,
                product_route,
                source_name,
            )

    sql, params = _build_sql_for_query(
        base_view=base_view,
        select=select,
        filter_=filter_,
        orderby=orderby,
        top=eff_top,
        skip=skip,
    )

    next_link = None
    if total_count is not None and eff_top is not None:
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

    # Non-streaming
    if not stream:
        with _DUCKDB_LOCK:
            cur = _DUCKDB_CONN.execute(sql, params)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description]

        records = [
            {col: val for col, val in zip(columns, row)}
            for row in rows
        ]

        response = {
            "@odata.context": f"/odata/$metadata#{product_route}/{source_name}",
            "value": records,
        }
        if total_count is not None:
            response["@odata.count"] = total_count
        if next_link:
            response["@odata.nextLink"] = next_link

        return response

    # Streaming
    def row_iterator():
        meta = {"@odata.context": f"/odata/$metadata#{product_route}/{source_name}"}
        if total_count is not None:
            meta["@odata.count"] = total_count
        if next_link:
            meta["@odata.nextLink"] = next_link

        head = json.dumps(meta, separators=(",", ":"))[:-1]
        yield head
        yield ',"value":['

        first = True
        with _DUCKDB_LOCK:
            cur = _DUCKDB_CONN.execute(sql, params)
            columns = [d[0] for d in cur.description]

            while True:
                chunk = cur.fetchmany(1000)
                if not chunk:
                    break
                for row in chunk:
                    if not first:
                        yield ","
                    else:
                        first = False
                    obj = {col: val for col, val in zip(columns, row)}
                    yield json.dumps(obj, default=str, separators=(",", ":"))

        yield "]}"  # close JSON

    return StreamingResponse(row_iterator(), media_type="application/json")
