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
)
indicators_router = APIRouter(prefix="/indicators", tags=["Indicators"])

@indicators_router.get("/sites")
async def list_sites(parquet_src: str):
    """
    List all available site names from the provided Parquet file.

    Args:
        parquet_src: Path to the Raven output Parquet file.

    Returns:
        List of site names.
    """
    con = duckdb.connect()
    try:
        sites = get_sites(con, parquet_src)
        return {"sites": sites}
    finally:
        con.close()

etl_router = APIRouter(prefix="/etl", tags=["ETL"])
indicators_router = APIRouter(prefix="/indicators", tags=["Indicators"])


@etl_router.post("/init")
async def initialize_etl(csv_path: str, output_path: str):
    """Initialize the ETL process for a Raven output CSV."""
    init_etl(csv_path, output_path)


@indicators_router.get("/")
async def get_indicators(
    parquet_src: str,
    efn_threshold: float = 0.2,
    break_point: Optional[int] = None,
    sites: Optional[List[str]] = Query(default=None),
):
    """Compute all flow indicators (optionally filtered by site)."""
    result_df = calculate_all_indicators(parquet_src, efn_threshold, break_point, sites)
    return result_df.to_dict(orient="records")


@indicators_router.get("/mean_annual_flow")
async def get_maf(parquet_src: str, sites: Optional[List[str]] = Query(default=None)):
    """Compute Mean Annual Flow for each site."""
    con = duckdb.connect()
    try:
        result_df = mean_annual_flow(con, parquet_src, sites)
        return result_df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/mean_aug_sep_flow")
async def get_mean_aug_sep_flow(parquet_src: str, sites: Optional[List[str]] = Query(default=None)):
    con = duckdb.connect()
    try:
        df = mean_aug_sep_flow(con, parquet_src, sites)
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/peak_flow_timing")
async def get_peak_flow_timing(parquet_src: str, sites: Optional[List[str]] = Query(default=None)):
    con = duckdb.connect()
    try:
        df = peak_flow_timing(con, parquet_src, sites)
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/days_below_efn")
async def get_days_below_efn(
    parquet_src: str,
    efn_threshold: float = 0.2,
    sites: Optional[List[str]] = Query(default=None),
):
    con = duckdb.connect()
    try:
        df = days_below_efn(con, parquet_src, efn_threshold, sites)
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/annual_peaks")
async def get_annual_peaks(parquet_src: str, sites: Optional[List[str]] = Query(default=None)):
    con = duckdb.connect()
    try:
        df = annual_peaks(con, parquet_src, sites)
        return df.to_dict(orient="records")
    finally:
        con.close()

@indicators_router.get("/weekly_flow_exceedance")
async def get_weekly_flow_exceedance(parquet_src: str, sites: Optional[List[str]] = Query(default=None)):
    con = duckdb.connect()
    try:
        df = weekly_flow_exceedance(con, parquet_src, sites)
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/peak_flows")
async def get_peak_flows(parquet_src: str, sites: Optional[List[str]] = Query(default=None)):
    con = duckdb.connect()
    try:
        df = peak_flows(con, parquet_src, sites)
        return df.to_dict(orient="records")
    finally:
        con.close()


@indicators_router.get("/flood_frequency_analysis")
async def get_flood_frequency_analysis(
    parquet_src: str,
    return_periods: str = "2,20",
    sites: Optional[List[str]] = Query(default=None),
):
    """Fit Flood Frequency Analysis (FFA) using Gumbel distribution."""
    con = duckdb.connect()
    try:
        peaks_df = annual_peaks(con, parquet_src, sites)
        rp_list = [int(rp.strip()) for rp in return_periods.split(",") if rp.strip().isdigit()]
        ffa_df = fit_ffa(peaks_df, return_periods=rp_list)
        return ffa_df.to_dict(orient="records")
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
