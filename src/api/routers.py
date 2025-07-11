from typing import Optional, List
from fastapi import APIRouter, Query
import duckdb
from src.raven_api.etl import init_etl
from src.raven_api.indicators import (
    calculate_all_indicators,
    mean_annual_flow,
    mean_aug_sep_flow,
    peak_flow_timing,
    days_below_efn,
    annual_peaks,
    peak_flows,
    weekly_flow_exceedance,
    fit_ffa,
    hydrograph,
)
indicators_router = APIRouter(prefix="/indicators", tags=["Indicators"])
etl_router = APIRouter(prefix="/etl", tags=["ETL"])

@etl_router.post("/init")
async def initialize_etl(csv_path: str, output_path: str):
    """Initialize the ETL process for a Raven output CSV."""
    init_etl(csv_path, output_path)


@indicators_router.get("/")
async def get_indicators(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    efn_threshold: float = Query(
        0.2,
        description="Environmental Flow Needs (EFN) threshold as a fraction of mean annual flow.",
        example=0.2
    ),
    break_point: Optional[int] = Query(
        None,
        description="Water year to split subperiods (e.g., 2000), or None for full period.",
        example=2000
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-01-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-12-31"
    ),
):
    """Compute all flow indicators (optionally filtered by site and date range)."""
    result_df = calculate_all_indicators(
        parquet_path=parquet_src,
        EFN_threshold=efn_threshold,
        break_point=break_point,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
    return result_df.to_dict(orient="records")


@indicators_router.get("/mean_annual_flow")
async def get_maf(
    parquet_src: str= Query(
        ...,
        description="Full local or remote path to a Parquet file."),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-02-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-03-08"
    ),
    temporal_resolution: str = Query(
        default="overall",
        description="Temporal resolution: 'overall' (single value) or 'annual' (one value per year).",
        example="annual"
    ),
):
    con = duckdb.connect()
    try:
        result_df = mean_annual_flow(
            con,
            parquet_src,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            temporal_resolution=temporal_resolution,
        )
        return result_df.to_dict(orient="records")
    finally:
        con.close()

@indicators_router.get("/peak_flow_timing")
async def get_peak_flow_timing(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-02-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-03-08"
    ),
    temporal_resolution: str = Query(
        default="overall",
        description="Temporal resolution: 'overall' (average across years) or 'annual' (one value per year).",
        example="annual"
    ),
):
    con = duckdb.connect()
    try:
        df = peak_flow_timing(
            con,
            parquet_src,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            temporal_resolution=temporal_resolution,
        )
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/days_below_efn")
async def get_days_below_efn(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    efn_threshold: float = Query(
        default=0.2,
        description="Environmental Flow Needs (EFN) threshold as a fraction of mean annual flow.",
        example=0.2
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-02-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-03-08"
    ),
    temporal_resolution: str = Query(
        default="overall",
        description="Temporal resolution: 'overall' (average across years) or 'annual' (one value per year).",
        example="annual"
    ),
):
    con = duckdb.connect()
    try:
        df = days_below_efn(
            con,
            parquet_src,
            EFN_threshold=efn_threshold,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            temporal_resolution=temporal_resolution,
        )
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/annual_peaks")
async def get_annual_peaks(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-01-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-12-31"
    ),
):
    con = duckdb.connect()
    try:
        df = annual_peaks(
            con,
            parquet_src,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        )
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/weekly_flow_exceedance")
async def get_weekly_flow_exceedance(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-01-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-12-31"
    ),
):
    con = duckdb.connect()
    try:
        df = weekly_flow_exceedance(
            con,
            parquet_src,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        )
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/peak_flows")
async def get_peak_flows(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-01-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-12-31"
    ),
):
    con = duckdb.connect()
    try:
        df = peak_flows(
            con,
            parquet_src,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        )
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/flood_frequency_analysis")
async def get_flood_frequency_analysis(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    return_periods: str = Query(
        default="2,20",
        description="Comma-separated list of return periods (e.g., '2,20,50').",
        example="2,20"
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-01-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-12-31"
    ),
):
    """Fit Flood Frequency Analysis (FFA) using Gumbel distribution."""
    con = duckdb.connect()
    try:
        peaks_df = annual_peaks(
            con,
            parquet_src,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        )
        rp_list = [int(rp.strip()) for rp in return_periods.split(",") if rp.strip().isdigit()]
        ffa_df = fit_ffa(peaks_df, return_periods=rp_list, sites=sites)
        return ffa_df.to_dict(orient="records")
    finally:
        con.close()

@indicators_router.get("/hydrograph")
async def get_hydrograph(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file."
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-01-01"
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-12-31"
    ),
    temporal_resolution: str = Query(
        "daily",
        description="Temporal resolution of the hydrograph. Choose from: 'daily', 'weekly', 'monthly', 'seasonal'."
    ),
):
    """
    Return a hydrograph (time series of flow) for the specified site(s), with optional time range filtering and temporal aggregation.
    """
    con = duckdb.connect()
    try:
        df = hydrograph(
            con,
            parquet_path=parquet_src,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            temporal_resolution=temporal_resolution
        )
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/sites")
async def list_sites(parquet_src: str):
    """List all available site names from the parquet file."""
    con = duckdb.connect()
    try:
        df = con.execute(
            f"SELECT DISTINCT site FROM parquet_scan('{parquet_src}') ORDER BY site"
        ).fetchdf()
        return df["site"].tolist()
    finally:
        con.close()
