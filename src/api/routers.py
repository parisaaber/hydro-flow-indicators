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
    return {"message": "ETL initialized"}


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
        0.2,
        description="EFN threshold as a fraction of mean annual flow.",
        example=0.2,
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
        default=1.5,
        description="IQR=1.5 (mild), Z-score=3, Modified Z=3.5",
    ),
    min_years: int = Query(default=5, description="Minimum years required for FFA"),
    selection_criteria: str = Query(
        default="aic", description="Best-fit criteria: 'aic','bic','ks','rmse'"
    ),
    debug: bool = Query(default=False, description="Enable debug output"),
):
    rp_list = [
        int(rp.strip()) for rp in return_periods.split(",") if rp.strip().isdigit()
    ]
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
    return df.to_dict(orient="records")


@indicators_router.get("/mean_annual_flow")
async def get_maf(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query(
        "overall", description="'overall' or 'annual'", example="annual"
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = mean_annual_flow(
        parquet_path,
        temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/mean_aug_sep_flow")
async def get_mean_aug_sep_flow(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query(
        "overall", description="'overall' or 'annual'", example="annual"
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = mean_aug_sep_flow(
        parquet_path,
        temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/peak_flow_timing")
async def get_peak_flow_timing(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query(
        "overall", description="'overall' or 'annual'", example="annual"
    ),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = peak_flow_timing(
        parquet_path,
        temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
    return df.to_dict(orient="records")


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
):
    df = days_below_efn(
        parquet_path,
        EFN_threshold=efn_threshold,
        temporal_resolution=temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/annual_peaks")
async def get_annual_peaks_route(
    parquet_path: str = Query(...),
    temporal_resolution: str = Query("overall", description="'overall' or 'annual'"),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = annual_peaks(
        parquet_path,
        temporal_resolution=temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/weekly_flow_exceedance")
async def get_weekly_flow_exceedance_route(
    parquet_path: str = Query(...),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = weekly_flow_exceedance(
        parquet_path, sites=sites, start_date=start_date, end_date=end_date
    )
    return df.to_dict(orient="records")


@indicators_router.get("/peak_flows")
async def get_peak_flows_route(
    parquet_path: str = Query(...),
    sites: Optional[List[str]] = Query(default=None),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    df = peak_flows(parquet_path, sites=sites, start_date=start_date, end_date=end_date)
    return df.to_dict(orient="records")


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
):
    rp_list = [
        int(rp.strip()) for rp in return_periods.split(",") if rp.strip().isdigit()
    ]
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
    )
    return df.to_dict(orient="records")


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
):
    df = aggregate_flows(
        parquet_path,
        temporal_resolution=temporal_resolution,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
    return df.to_dict(orient="records")


@indicators_router.get("/sites")
async def list_sites(
    parquet_path: str = Query(
        ..., description="Full local or remote path to a Parquet file."
    ),
    prefix: Optional[str] = Query(
        None, description="Optional prefix to filter site IDs."
    ),
):
    """
    List all available site names from the Parquet file,
    optionally filtering by a prefix.
    """
    main_view = CXN.get_or_create_main_view(parquet_path)

    if prefix:
        esc = prefix.replace("'", "''")
        q = f"""
            SELECT DISTINCT site
            FROM {main_view}
            WHERE site ILIKE '{esc}%'
            ORDER BY site
        """
    else:
        q = f"SELECT DISTINCT site FROM {main_view} ORDER BY site"

    df = CXN.execute(q).fetchdf()
    return df["site"].tolist()


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
