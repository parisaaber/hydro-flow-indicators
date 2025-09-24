from typing import Annotated, List, Optional
from fastapi import APIRouter, Depends, Query
from ..raven_api.etl import init_etl
from ..raven_api.indicators import CXN
from ..raven_api.indicators import (
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
    get_connection_status,
    reset_connection,
    configure_connection,
)
from src.raven_api.mapping import map_features

etl_router = APIRouter(tags=["ETL"])
indicators_router = APIRouter(tags=["Indicators"])
mapping_router = APIRouter(tags=["Mapping"])
connection_router = APIRouter(tags=["Connection"])


@etl_router.post(
    "/init", description="Initialize the ETL process for a Raven output CSV."
)
async def initialize_etl(
    csv_path: str,
    output_path: str,
    spatial_path: str = None,
    join_column: str = None
):
    init_etl(csv_path, output_path, spatial_path, join_column)


def common_parameters(
    parquet_src: str = Query(
        ...,
        description="Full local or remote path to a Parquet file.",
    ),
    sites: Optional[List[str]] = Query(
        default=None,
        description="List of site IDs.",
        example=["sub11004314 [m3/s]"],
    ),
    start_date: Optional[str] = Query(
        default=None,
        description="Start date for filtering (format: YYYY-MM-DD).",
        example="2010-01-01",
    ),
    end_date: Optional[str] = Query(
        default=None,
        description="End date for filtering (format: YYYY-MM-DD).",
        example="2015-12-31",
    ),
) -> dict:
    return {
        "parquet_src": parquet_src,
        "sites": sites,
        "start_date": start_date,
        "end_date": end_date,
    }


CommonsDep = Annotated[dict, Depends(common_parameters)]


@connection_router.post("/configure")
async def configure_global_connection(
    parquet_path: str = Query(..., description="Path to parquet file"),
    sites: Optional[List[str]] = Query(
        default=None,
        description="Site IDs to filter"
    ),
    start_date: Optional[str] = Query(
        default=None, description="Start date (YYYY-MM-DD)"
    ),
    end_date: Optional[str] = Query(
        default=None, description="End date (YYYY-MM-DD)"),
):
    """Configure the global DuckDB connection with parquet file and filters"""
    configure_connection(
        parquet_path=parquet_path,
        sites=sites,
        start_date=start_date,
        end_date=end_date
    )
    return {
        "message": "Connection configured successfully",
        "config": get_connection_status(),
    }


@connection_router.get("/status")
async def get_connection_info():
    """Get current connection configuration and available views"""
    return get_connection_status()


@connection_router.post("/reset")
async def reset_global_connection():
    """Reset the global connection configuration"""
    reset_connection()
    return {"message": "Connection reset successfully"}


@indicators_router.get("/")
async def get_indicators(
    efn_threshold: float = Query(
        0.2,
        description=(
            "Environmental Flow Needs (EFN) threshold as a fraction "
            "of mean annual flow."
        ),
        example=0.2,
    ),
    return_periods: str = Query(
        default="2,20",
        description=(
            "Comma-separated list of return periods for FFA "
            "(e.g., '2,20,50')"
        ),
        example="2,20",
    ),
    dist: str = Query(
        default="auto",
        description=(
            "Distribution to fit for FFA ('genextreme', 'logpearson3', "
            "'lognormal', 'gumbel', 'gamma', 'normal', or 'auto' for best-fit)"
        ),
    ),
    remove_outliers: bool = Query(
        default=False,
        description="Whether to remove outliers before FFA fitting",
    ),
    outlier_method: str = Query(
        default="iqr",
        description=(
            "Outlier detection method ('iqr', 'zscore', 'modified_zscore')"
        ),
    ),
    outlier_threshold: float = Query(
        default=1.5,
        description=(
            "Threshold for outlier detection: IQR=1.5 (mild outlier), "
            "Z-score=3, Modified Z-score=3.5"
        ),
    ),
    min_years: int = Query(
        default=5,
        description="Minimum number of years required for FFA",
    ),
    selection_criteria: str = Query(
        default="aic",
        description=(
            "Criteria for selecting best distribution"
            "('aic', 'bic', 'ks', 'rmse')"
        ),
    ),
    debug: bool = Query(
        default=False,
        description="Enable debug output for troubleshooting",
    ),
):
    """Compute all flow indicators using the globally configured connection."""

    # Parse return periods
    rp_list = [
        int(
            rp.strip()
            ) for rp in return_periods.split(",") if rp.strip().isdigit()
    ]

    result_df = calculate_all_indicators(
        EFN_threshold=efn_threshold,
        return_periods=rp_list,
        remove_outliers=remove_outliers,
        outlier_method=outlier_method,
        outlier_threshold=outlier_threshold,
        dist=dist,
        selection_criteria=selection_criteria,
        min_years=min_years,
        debug=debug,
    )

    return result_df.to_dict(orient="records")


@indicators_router.get("/mean_annual_flow")
async def get_maf(
    temporal_resolution: str = Query(
        default="overall",
        description="Temporal resolution: 'overall' (single value)"
        "or 'annual' (one value per year).",
        example="annual",
    ),
):
    result_df = mean_annual_flow(
        temporal_resolution=temporal_resolution,
    )
    return result_df.to_dict(orient="records")


@indicators_router.get("/mean_aug_sep_flow")
async def get_mean_aug_sep_flow(
    temporal_resolution: str = Query(
        default="overall",
        description="Temporal resolution: 'overall' (single value)"
        "or 'annual' (one value per year).",
        example="annual",
    ),
):
    """Compute mean flow for August-September (overall or per year)."""
    df = mean_aug_sep_flow(
        temporal_resolution=temporal_resolution,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/peak_flow_timing")
async def get_peak_flow_timing(
    temporal_resolution: str = Query(
        default="overall",
        description="Temporal resolution: 'overall' (average across years)"
        "or 'annual' (one value per year).",
        example="annual",
    ),
):
    """Compute peak flow timing (overall or per year)"
    "for selected sites and time range."""

    df = peak_flow_timing(
        temporal_resolution=temporal_resolution,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/days_below_efn")
async def get_days_below_efn(
    efn_threshold: float = Query(
        default=0.2,
        description="Environmental Flow Needs (EFN) threshold"
        "as a fraction of mean annual flow.",
        example=0.2,
    ),
    temporal_resolution: str = Query(
        default="overall",
        description="Temporal resolution: 'overall' (average across years)"
        "or 'annual' (one value per year).",
        example="annual",
    ),
):
    """Compute the number of days below EFN for"
    "selected sites and time range."""
    df = days_below_efn(
        EFN_threshold=efn_threshold,
        temporal_resolution=temporal_resolution,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/annual_peaks")
async def get_annual_peaks():
    """Retrieve annual peak flows for the specified sites and period."""
    df = annual_peaks()
    return df.to_dict(orient="records")


@indicators_router.get("/weekly_flow_exceedance")
async def get_weekly_flow_exceedance():
    """Compute weekly flow exceedance for the specified sites and period."""
    df = weekly_flow_exceedance()
    return df.to_dict(orient="records")


@indicators_router.get("/peak_flows")
async def get_peak_flows():
    """Retrieve peak flows for the specified sites and period."""

    df = peak_flows()
    return df.to_dict(orient="records")


@indicators_router.get("/flood_frequency_analysis")
async def get_flood_frequency_analysis(
    return_periods: str = Query(
        default="2,20",
        description="Comma-separated list of return periods (e.g., '2,20,50')",
        example="2,20",
    ),
    dist: str = Query(
        default="auto",
        description="Distribution to fit "
        "( 'genextreme' , 'logpearson3' , 'lognormal' , "
        "'gumbel', 'gamma' , 'normal', or 'auto' for best-fit)",
    ),
    remove_outliers: bool = Query(
        default=False, description="Whether to remove outliers before fitting"
    ),
    outlier_method: str = Query(
        default="iqr",
        description="Outlier detection method "
        "('iqr', 'zscore', 'modified_zscore')",
    ),
    outlier_threshold: float = Query(
        default=1.5,
        description="Suggested thresholds for outlier detection: "
        "IQR=1.5 (mild outlier), Z-score=3, Modified Z-score=3.5",
    ),
    min_years: int = Query(
        default=5, description="Minimum number of years required for analysis"
    ),
    selection_criteria: str = Query(
        default="aic",
        description="Criteria for selecting best distribution"
        "('aic', 'bic', 'ks', 'rmse')",
    ),
):
    """Fit Flood Frequency Analysis (FFA) using
    the enhanced fit_ffa function with global CXN."""

    # Parse return periods
    rp_list = [
        int(
            rp.strip()
            ) for rp in return_periods.split(",") if rp.strip().isdigit()
    ]

    # Call the CXN-based fit_ffa
    ffa_df = fit_ffa(
        dist=dist,
        return_periods=rp_list,
        remove_outliers=remove_outliers,
        outlier_method=outlier_method,
        outlier_threshold=outlier_threshold,
        selection_criteria=selection_criteria,
        min_years=min_years,
        debug=False,
    )

    return ffa_df.to_dict(orient="records")


@indicators_router.get("/aggregate_flows")
async def get_aggregate_flows(
    temporal_resolution: str = Query(
        "daily",
        description="Temporal resolution of the hydrograph."
        "Choose from: 'daily', 'weekly', 'monthly', 'seasonal'.",
    ),
):
    """
    Return a hydrograph (time series of flow) for the specified site(s),"
    "with optional time range filtering and temporal aggregation.
    """
    df = aggregate_flows(temporal_resolution=temporal_resolution)
    return df.to_dict(orient="records")


@indicators_router.get("/sites")
async def list_sites():
    """List all available site names from the globally configured CXN."""

    # Use CXN to create or access the filtered data view
    filtered_view = CXN.create_filtered_data_view("temp_sites_view")

    query = f"""
        SELECT DISTINCT site
        FROM {filtered_view}
        ORDER BY site
    """

    df = CXN.execute(query).fetchdf()
    return df["site"].tolist()


@mapping_router.get("/features")
async def get_features(
    geojson_src: str = Query(
        ...,
        description="Full local or remote path to a geojson file.",
    )
):
    return map_features(geojson_src)
