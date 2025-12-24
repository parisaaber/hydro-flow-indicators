from typing import List, Optional
from pydantic import BaseModel, Field, Dict
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
from raven_api.utils import envelope

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


class IndicatorsBaseRequest(BaseModel):
    parquet_path: str = Field(
        ..., description="Full local or remote path to a Parquet file."
    )
    sites: Optional[List[str]] = Field(default=None, description="List of site IDs.")
    start_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    # pagination
    limit: Optional[int] = Field(default=None, ge=1, le=10000)
    cursor: Optional[Dict[str, str]] = Field(
        default=None,
        description='Cursor object, e.g. {"site":"S"}.',
    )


class AllIndicatorsRequest(IndicatorsBaseRequest):
    efn_threshold: float = Field(
        default=0.2,
        description="EFN threshold as a fraction of mean annual flow.",
        examples=[0.2],
    )
    return_periods: str = Field(
        default="2,20",
        description="Comma-separated return periods for FFA (e.g., '2,20,50')",
        examples=["2,20"],
    )
    dist: str = Field(
        default="auto",
        description="Distribution for FFA ('genextreme','logpearson3','lognormal','gumbel','gamma','normal','auto')",
    )
    remove_outliers: bool = Field(
        default=False, description="Remove outliers before FFA fitting"
    )
    outlier_method: str = Field(
        default="iqr", description="Outlier method: 'iqr','zscore','modified_zscore'"
    )
    outlier_threshold: float = Field(
        default=1.5, description="IQR=1.5 (mild), Z-score=3, Modified Z=3.5"
    )
    min_years: int = Field(default=5, description="Minimum years required for FFA")
    selection_criteria: str = Field(
        default="aic", description="Best-fit criteria: 'aic','bic','ks','rmse'"
    )
    debug: bool = Field(default=False, description="Enable debug output")


class TemporalResolutionRequest(IndicatorsBaseRequest):
    temporal_resolution: str = Field(
        default="overall",
        description="'overall' or 'annual'",
        examples=["annual"],
    )


class DaysBelowEFNRequest(IndicatorsBaseRequest):
    efn_threshold: float = Field(
        default=0.2, description="EFN threshold as a fraction of MAF."
    )
    temporal_resolution: str = Field(
        default="overall",
        description="'overall' or 'annual'",
        examples=["annual"],
    )


class WeeklyFlowExceedanceRequest(IndicatorsBaseRequest):
    pass


class PeakFlowsRequest(IndicatorsBaseRequest):
    pass


class FloodFrequencyAnalysisRequest(IndicatorsBaseRequest):
    return_periods: str = Field(
        default="2,20",
        description="Comma-separated return periods (e.g., '2,20,50')",
    )
    dist: str = Field(default="auto", description="Distribution for FFA")
    remove_outliers: bool = Field(default=False)
    outlier_method: str = Field(default="iqr")
    outlier_threshold: float = Field(default=1.5)
    min_years: int = Field(default=5)
    selection_criteria: str = Field(default="aic")


class AggregateFlowsRequest(IndicatorsBaseRequest):
    temporal_resolution: str = Field(
        default="daily",
        description="Hydrograph resolution: 'daily', 'weekly', 'monthly', 'seasonal'.",
    )


@indicators_router.post("/", description="Compute all flow indicators.")
async def get_indicators(payload: AllIndicatorsRequest):
    rp_list = [
        int(rp.strip())
        for rp in payload.return_periods.split(",")
        if rp.strip().isdigit()
    ]
    cur = payload.cursor

    if payload.limit is None:
        df = calculate_all_indicators(
            parquet_path=payload.parquet_path,
            EFN_threshold=payload.efn_threshold,
            return_periods=rp_list,
            remove_outliers=payload.remove_outliers,
            outlier_method=payload.outlier_method,
            outlier_threshold=payload.outlier_threshold,
            dist=payload.dist,
            selection_criteria=payload.selection_criteria,
            min_years=payload.min_years,
            debug=payload.debug,
            sites=payload.sites,
            start_date=payload.start_date,
            end_date=payload.end_date,
        )
        return envelope(df.to_dict(orient="records"))

    main = CXN.get_or_create_main_view(payload.parquet_path)

    where_parts = []
    if payload.sites:
        esc_sites = ", ".join("'" + s.replace("'", "''") + "'" for s in payload.sites)
        where_parts.append(f"site IN ({esc_sites})")
    if payload.start_date and payload.end_date:
        where_parts.append(
            f"date BETWEEN '{payload.start_date}' AND '{payload.end_date}'"
        )
    if cur and "site" in cur:
        esc = str(cur["site"]).replace("'", "''")
        where_parts.append(
            f"(lower(site) > lower('{esc}') OR (lower(site) = lower('{esc}') AND site > '{esc}'))"
        )

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    q_sites = f"""
      WITH distinct_sites AS (
        SELECT DISTINCT site
        FROM {main}
        {where_sql}
      )
      SELECT site
      FROM distinct_sites
      ORDER BY lower(site), site
      LIMIT {payload.limit + 1}
    """
    rows = CXN.execute(q_sites).fetchall()
    page_sites = [r[0] for r in rows[: payload.limit]]
    has_more = len(rows) > payload.limit
    next_cur = {"site": page_sites[-1]} if has_more else None

    df = calculate_all_indicators(
        parquet_path=payload.parquet_path,
        EFN_threshold=payload.efn_threshold,
        return_periods=rp_list,
        remove_outliers=payload.remove_outliers,
        outlier_method=payload.outlier_method,
        outlier_threshold=payload.outlier_threshold,
        dist=payload.dist,
        selection_criteria=payload.selection_criteria,
        min_years=payload.min_years,
        debug=payload.debug,
        sites=page_sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
    )

    return envelope(df.to_dict(orient="records"), has_more, next_cur)


@indicators_router.post("/mean_annual_flow")
async def get_maf(payload: TemporalResolutionRequest):
    cur_obj = payload.cursor

    df = mean_annual_flow(
        payload.parquet_path,
        payload.temporal_resolution,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]

    if payload.temporal_resolution == "annual":
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


@indicators_router.post("/mean_aug_sep_flow")
async def get_mean_aug_sep_flow(payload: TemporalResolutionRequest):
    cur_obj = payload.cursor

    df = mean_aug_sep_flow(
        payload.parquet_path,
        payload.temporal_resolution,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]

    if payload.temporal_resolution == "annual":
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


@indicators_router.post("/peak_flow_timing")
async def get_peak_flow_timing(payload: TemporalResolutionRequest):
    cur_obj = payload.cursor

    df = peak_flow_timing(
        payload.parquet_path,
        payload.temporal_resolution,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]

    if payload.temporal_resolution == "annual":
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


@indicators_router.post("/days_below_efn")
async def get_days_below_efn_route(payload: DaysBelowEFNRequest):
    cur_obj = payload.cursor

    df = days_below_efn(
        payload.parquet_path,
        EFN_threshold=payload.efn_threshold,
        temporal_resolution=payload.temporal_resolution,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]

    if payload.temporal_resolution == "annual":
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


@indicators_router.post("/annual_peaks")
async def get_annual_peaks_route(payload: TemporalResolutionRequest):
    cur_obj = payload.cursor

    df = annual_peaks(
        payload.parquet_path,
        temporal_resolution=payload.temporal_resolution,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]

    if payload.temporal_resolution == "annual":
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


@indicators_router.post("/weekly_flow_exceedance")
async def get_weekly_flow_exceedance_route(payload: WeeklyFlowExceedanceRequest):
    cur_obj = payload.cursor

    df = weekly_flow_exceedance(
        payload.parquet_path,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]

    next_cur = (
        {"site": str(page.iloc[-1]["site"]), "week": int(page.iloc[-1]["week"])}
        if has_more
        else None
    )
    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.post("/peak_flows")
async def get_peak_flows_route(payload: PeakFlowsRequest):
    cur_obj = payload.cursor

    df = peak_flows(
        payload.parquet_path,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]
    next_cur = {"site": str(page.iloc[-1]["site"])} if has_more else None
    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.post("/flood_frequency_analysis")
async def get_flood_frequency_analysis(payload: FloodFrequencyAnalysisRequest):
    rp_list = [
        int(rp.strip())
        for rp in payload.return_periods.split(",")
        if rp.strip().isdigit()
    ]
    cur_obj = payload.cursor

    df = fit_ffa(
        payload.parquet_path,
        dist=payload.dist,
        return_periods=rp_list,
        remove_outliers=payload.remove_outliers,
        outlier_method=payload.outlier_method,
        outlier_threshold=payload.outlier_threshold,
        selection_criteria=payload.selection_criteria,
        min_years=payload.min_years,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        debug=False,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]
    next_cur = (
        {
            "flow_rank": int(page.iloc[-1]["flow_rank"]),
            "site": str(page.iloc[-1]["site"]),
        }
        if has_more
        else None
    )
    return envelope(page.to_dict(orient="records"), has_more, next_cur)


@indicators_router.post("/aggregate_flows")
async def get_aggregate_flows_route(payload: AggregateFlowsRequest):
    cur_obj = payload.cursor

    df = aggregate_flows(
        payload.parquet_path,
        temporal_resolution=payload.temporal_resolution,
        sites=payload.sites,
        start_date=payload.start_date,
        end_date=payload.end_date,
        limit=payload.limit,
        cursor=cur_obj,
    )

    if payload.limit is None:
        return envelope(df.to_dict(orient="records"))

    has_more = len(df) > payload.limit
    page = df.iloc[: payload.limit]
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


class ListSitesRequest(BaseModel):
    parquet_path: str = Field(
        ..., description="Full local or remote path to a Parquet file."
    )
    prefix: Optional[str] = Field(
        default=None, description="Optional prefix to filter site IDs."
    )
    limit: Optional[int] = Field(
        default=None, ge=1, le=10000, description="Max number of sites to return."
    )
    cursor: Optional[Dict[str, SystemError]] = Field(
        default=None,
        description='Cursor object, e.g. {"site":"S"}.',
    )


@indicators_router.post("/sites")
async def list_sites(payload: ListSitesRequest):
    cur = payload.cursor
    main = CXN.get_or_create_main_view(payload.parquet_path)

    wh = []
    if payload.prefix:
        esc = payload.prefix.replace("'", "''")
        wh.append(f"site ILIKE '{esc}%'")
    if cur and "site" in cur:
        esc = str(cur["site"]).replace("'", "''")
        wh.append(
            f"(lower(site) > lower('{esc}') OR (lower(site) = lower('{esc}') AND site > '{esc}'))"
        )

    where = f"WHERE {' AND '.join(wh)}" if wh else ""
    limit_sql = f"LIMIT {payload.limit + 1}" if payload.limit is not None else ""

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

    if payload.limit is None:
        items = [r[0] for r in rows]
        return envelope(items, has_more=False, next_cursor=None)

    items = [r[0] for r in rows[: payload.limit]]
    has_more = len(rows) > payload.limit
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
