from typing import List, Optional
from fastapi import APIRouter, Query
from raven_api.etl import init_etl
from raven_api.indicators import (
    calculate_all_indicators,
    days_below_efn,
    fit_ffa,
    annual_peaks,
    aggregate_flows,
    mean_aug_sep_flow,
    mean_annual_flow,
    peak_flow_timing,
    peak_flows,
    weekly_flow_exceedance,
    CXN,
)
from raven_api.mapping import map_features
from raven_api.utils import envelope, parse_cursor

etl_router = APIRouter(tags=["ETL"])
indicators_router = APIRouter(tags=["Indicators"])
mapping_router = APIRouter(tags=["Mapping"])
connection_router = APIRouter(tags=["Connection"])


# -----------------------
# ETL
# -----------------------
@etl_router.post(
    "/init", description="Initialize the ETL process for a Raven output CSV."
)
async def initialize_etl(
    csv_path: str,
    output_path: str,
    spatial_path: Optional[str] = None,
    join_column: Optional[str] = None,
):
    init_etl(csv_path, output_path, spatial_path, join_column)
    return {"message": "ETL Done"}


# -----------------------
# Connection
# -----------------------
@connection_router.post(
    "/reset", description="Clear cached dataset views in the DuckDB connection."
)
async def reset_global_connection():
    CXN.reset()
    return {"message": "Connection cache reset successfully"}


# -----------------------
# Indicators
# -----------------------
@indicators_router.get("/", description="Compute all flow indicators.")
async def get_indicators(
    parquet_path: str = Query(
        ..., description="Full local or remote path to a Parquet file."
    ),
    sites: Optional[List[str]] = Query(default=None, description="List of site IDs."),
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    efn_threshold: float = Query(
        0.2, description="EFN threshold as a fraction of mean annual flow.", example=0.2
    ),
    return_periods: str = Query(
        default="2,20",
        description="Comma-separated return periods for FFA (e.g., '2,20,50')",
        example="2,20",
    ),
    dist: str = Query(
        default="auto",
        description="Distribution for FFA ('genextreme','logpearson3','lognormal','gumbel','gamma','normal','auto')",
    ),
    remove_outliers: bool = Query(
        default=False, description="Remove outliers before FFA fitting"
    ),
    outlier_method: str = Query(
        default="iqr", description="Outlier method: 'iqr','zscore','modified_zscore'"
    ),
    outlier_threshold: float = Query(
        default=1.5, description="IQR=1.5 (mild), Z-score=3, Modified Z=3.5"
    ),
    min_years: int = Query(default=5, description="Minimum years required for FFA"),
    selection_criteria: str = Query(
        default="aic", description="Best-fit criteria: 'aic','bic','ks','rmse'"
    ),
    debug: bool = Query(default=False, description="Enable debug output"),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string cursor like {"site":"S"}.',
    ),
):
    rp_list = [
        int(rp.strip()) for rp in return_periods.split(",") if rp.strip().isdigit()
    ]
    cur = parse_cursor(cursor)

    if limit is None:
        df = calculate_all_indicators(
            parquet_path=parquet_path,
            EFN_threshold=efn_threshold,
            return_periods=rp_list,
            remove_outliers=remove_outliers,
            outlier_method=outlier_method,
            outlier_threshold=outlier_threshold,
            dist=dist,
            selection_criteria=selection_criteria,
            min_years=min_years,
            debug=debug,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        )
        return envelope(df.to_dict(orient="records"))

    main = CXN.get_or_create_main_view(parquet_path)

    where_parts = []
    if sites:
        esc_sites = ", ".join("'" + s.replace("'", "''") + "'" for s in sites)
        where_parts.append(f"site IN ({esc_sites})")
    if start_date and end_date:
        where_parts.append(f"date BETWEEN '{start_date}' AND '{end_date}'")
    if cur and "site" in cur:
        esc = str(cur["site"]).replace("'", "''")
        where_parts.append(
            f"(lower(site) > lower('{esc}') OR (lower(site) = lower('{esc}') AND site > '{esc}'))"
        )

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    # Get page of sites
    q_sites = f"""
      WITH distinct_sites AS (
        SELECT DISTINCT site
        FROM {main}
        {where_sql}
      )
      SELECT site
      FROM distinct_sites
      ORDER BY lower(site), site
      LIMIT {limit + 1}
    """
    rows = CXN.execute(q_sites).fetchall()
    page_sites = [r[0] for r in rows[:limit]]
    has_more = len(rows) > limit
    next_cur = {"site": page_sites[-1]} if has_more else None

    # Compute indicators ONLY for this page of sites
    df = calculate_all_indicators(
        parquet_path=parquet_path,
        EFN_threshold=efn_threshold,
        return_periods=rp_list,
        remove_outliers=remove_outliers,
        outlier_method=outlier_method,
        outlier_threshold=outlier_threshold,
        dist=dist,
        selection_criteria=selection_criteria,
        min_years=min_years,
        debug=debug,
        sites=page_sites,
        start_date=start_date,
        end_date=end_date,
    )

    return envelope(df.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/mean_annual_flow")
async def get_maf(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query(
        "overall", description="'overall' or 'annual'", example="annual"
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string. overall={"site":"S"}; annual={"site":"S","water_year":2001}',
    ),
):
    cur_obj = parse_cursor(cursor)

    df = mean_annual_flow(
        parquet_path,
        temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]

    if temporal_resolution == "annual":
        next_cur = (
            {
                "site": str(page.iloc[-1]["site"]),
                "water_year": int(page.iloc[-1]["water_year"]),
            }
            if has_more
            else None
        )
    else:
        next_cur = {"site": str(page.iloc[-1]["site"])} if has_more else None

    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/mean_aug_sep_flow")
async def get_mean_aug_sep_flow(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query(
        "overall", description="'overall' or 'annual'", example="annual"
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string. overall={"site":"S"}; annual={"site":"S","water_year":2001}',
    ),
):
    cur_obj = parse_cursor(cursor)
    df = mean_aug_sep_flow(
        parquet_path,
        temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,
    )
    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]

    if temporal_resolution == "annual":
        next_cur = (
            {
                "site": str(page.iloc[-1]["site"]),
                "water_year": int(page.iloc[-1]["water_year"]),
            }
            if has_more
            else None
        )
    else:
        next_cur = {"site": str(page.iloc[-1]["site"])} if has_more else None

    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/peak_flow_timing")
async def get_peak_flow_timing(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query(
        "overall", description="'overall' or 'annual'", example="annual"
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string. overall={"site":"S"}; annual={"site":"S","water_year":2001}',
    ),
):
    cur_obj = parse_cursor(cursor)

    df = peak_flow_timing(
        parquet_path,
        temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,  # {"site": "..."} or {"site":"...","water_year": 2001}
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]

    if temporal_resolution == "annual":
        next_cur = (
            {
                "site": str(page.iloc[-1]["site"]),
                "water_year": int(page.iloc[-1]["water_year"]),
            }
            if has_more
            else None
        )
    else:
        next_cur = {"site": str(page.iloc[-1]["site"])} if has_more else None

    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/days_below_efn")
async def get_days_below_efn_route(
    parquet_path: str = Query(...),
    efn_threshold: float = Query(
        0.2, description="EFN threshold as a fraction of MAF."
    ),
    temporal_resolution: str = Query(
        "overall", description="'overall' or 'annual'", example="annual"
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string. overall={"site":"S"}; annual={"site":"S","water_year":2001}',
    ),
):
    cur_obj = parse_cursor(cursor)

    df = days_below_efn(
        parquet_path,
        EFN_threshold=efn_threshold,
        temporal_resolution=temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,  # {"site": "..."} or {"site":"...","water_year": 2001}
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]

    if temporal_resolution == "annual":
        next_cur = (
            {
                "site": str(page.iloc[-1]["site"]),
                "water_year": int(page.iloc[-1]["water_year"]),
            }
            if has_more
            else None
        )
    else:
        next_cur = {"site": str(page.iloc[-1]["site"])} if has_more else None

    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/annual_peaks")
async def get_annual_peaks_route(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query("overall", description="'overall' or 'annual'"),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string. overall={"site":"S"}; annual={"site":"S","water_year":2001}',
    ),
):
    cur_obj = parse_cursor(cursor)

    df = annual_peaks(
        parquet_path,
        temporal_resolution=temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,  # {"site": "..."} or {"site":"...","water_year": 2001}
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]

    if temporal_resolution == "annual":
        next_cur = (
            {
                "site": str(page.iloc[-1]["site"]),
                "water_year": int(page.iloc[-1]["water_year"]),
            }
            if has_more
            else None
        )
    else:
        next_cur = {"site": str(page.iloc[-1]["site"])} if has_more else None

    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/weekly_flow_exceedance")
async def get_weekly_flow_exceedance_route(
    parquet_path: str = Query(...),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string cursor like {"site":"S","week":23}',
    ),
):
    cur_obj = parse_cursor(cursor)

    df = weekly_flow_exceedance(
        parquet_path,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,  # {"site":"...","week": <0-53>}
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]

    next_cur = (
        {"site": str(page.iloc[-1]["site"]), "week": int(page.iloc[-1]["week"])}
        if has_more
        else None
    )
    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/peak_flows")
async def get_peak_flows_route(
    parquet_path: str = Query(...),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string cursor like {"site":"S"}',
    ),
):
    cur_obj = parse_cursor(cursor)

    df = peak_flows(
        parquet_path,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,  # {"site":"..."}
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]
    next_cur = {"site": str(page.iloc[-1]["site"])} if has_more else None
    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/flood_frequency_analysis")
async def get_flood_frequency_analysis(
    parquet_path: str = Query(...),
    return_periods: str = Query(
        "2,20", description="Comma-separated return periods (e.g., '2,20,50')"
    ),
    dist: str = Query("auto", description="Distribution for FFA"),
    remove_outliers: bool = Query(False),
    outlier_method: str = Query("iqr"),
    outlier_threshold: float = Query(1.5),
    min_years: int = Query(5),
    selection_criteria: str = Query("aic"),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string cursor like {"flow_rank": 25, "site": "ABC123"}',
    ),
):
    rp_list = [
        int(rp.strip()) for rp in return_periods.split(",") if rp.strip().isdigit()
    ]
    cur_obj = parse_cursor(cursor)

    df = fit_ffa(
        parquet_path,
        dist=dist,
        return_periods=rp_list,
        remove_outliers=remove_outliers,
        outlier_method=outlier_method,
        outlier_threshold=outlier_threshold,
        selection_criteria=selection_criteria,
        min_years=min_years,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        debug=False,
        limit=limit,
        cursor=cur_obj,  # {"flow_rank": int, "site": str}
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]
    next_cur = (
        {
            "flow_rank": int(page.iloc[-1]["flow_rank"]),
            "site": str(page.iloc[-1]["site"]),
        }
        if has_more
        else None
    )
    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.get("/aggregate_flows")
async def get_aggregate_flows_route(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query(
        "daily",
        description="Hydrograph resolution: 'daily', 'weekly', 'monthly', 'seasonal'.",
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    # pagination
    limit: Optional[int] = Query(None, ge=1, le=10000),
    cursor: Optional[str] = Query(
        None,
        description='JSON string cursor like {"site":"S","time_period":"YYYY-MM-DD|YYYY-ww|YYYY-mm|YYYY-Season"}',
    ),
):
    cur_obj = parse_cursor(cursor)

    df = aggregate_flows(
        parquet_path,
        temporal_resolution=temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        cursor=cur_obj,  # {"site":"...","time_period":"..."}
    )

    if limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > limit
    page = df.iloc[:limit]
    next_cur = (
        {
            "site": str(page.iloc[-1]["site"]),
            "time_period": str(page.iloc[-1]["time_period"]),
        }
        if has_more
        else None
    )
    return envelope(page.to_dict(orient="records"), has_more, next_cur)


# -----------------------
# Sites
# -----------------------
@indicators_router.get("/sites")
async def list_sites(
    parquet_path: str = Query(
        ..., description="Full local or remote path to a Parquet file."
    ),
    prefix: Optional[str] = Query(
        None, description="Optional prefix to filter site IDs."
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        le=10000,
        description="Max number of sites to return.",
    ),
    cursor: Optional[str] = Query(
        None,
        description='Return sites strictly after this ID, under the form of a JSON string like {"site": "<site_id>"}.',
    ),
):
    cur = parse_cursor(cursor)
    main = CXN.get_or_create_main_view(parquet_path)

    wh = []
    if prefix:
        esc = prefix.replace("'", "''")
        wh.append(f"site ILIKE '{esc}%'")
    if cur and "site" in cur:
        esc = str(cur["site"]).replace("'", "''")
        wh.append(
            f"(lower(site) > lower('{esc}') OR (lower(site) = lower('{esc}') AND site > '{esc}'))"
        )

    where = f"WHERE {' AND '.join(wh)}" if wh else ""
    limit_sql = f"LIMIT {limit + 1}" if limit is not None else ""

    q = f"""
      WITH distinct_sites AS (
        SELECT DISTINCT site
        FROM {main}
        {where}
      )
      SELECT site
      FROM distinct_sites
      ORDER BY lower(site), site
      {limit_sql}
    """
    rows = CXN.execute(q).fetchall()
    if limit is None:
        items = [r[0] for r in rows]
        return envelope(items, has_more=False, next_cursor=None)

    items = [r[0] for r in rows[:limit]]
    has_more = len(rows) > limit
    next_cur = {"site": items[-1]} if has_more and items else None
    return envelope(items, has_more, next_cur)


# -----------------------
# Mapping
# -----------------------
@mapping_router.get("/features")
async def get_features(
    geojson_src: str = Query(
        ..., description="Full local or remote path to a GeoJSON file."
    ),
):
    return map_features(geojson_src)
