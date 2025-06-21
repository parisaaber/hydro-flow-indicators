from typing import Optional
from fastapi import APIRouter

from raven_api.etl import init_etl
from raven_api.indicators import calculate_all_indicators, mean_annual_flow


etl_router = APIRouter(prefix="/etl", tags=["ETL"])
indicators_router = APIRouter(prefix="/indicators", tags=["Indicators"])


@etl_router.post("/init")
async def initialize_etl(csv_path: str, output_path: str):
    """
    Initialize the ETL process for a Raven output

    Args:
        csv_path: Path to the input CSV file (remote or local).
        output_path: Path to save the processed Parquet file (remote or local).
    """
    init_etl(csv_path, output_path)


@indicators_router.get("/")
async def get_indicators(
    parquet_src: str, efn_threshold: float = 0.2, break_point: Optional[int] = None
):
    """
    Compute flow indicators

    Args:
        parquet_src: Raven output parquet file.
        efn_threshold: EFN threshold between 0 and 1 (Defaults to 0.2).
        break_point: Optional water year to split the period into subperiods.

    Returns:
        List of dictionaries with calculated indicators.
    """
    result_df = calculate_all_indicators(parquet_src, efn_threshold, break_point)

    return result_df.to_dict(orient="records")


@indicators_router.get("/mean_annual_flow")
async def get_maf(parquet_src: str):
    """
    Compute Mean Annual Flow for each site.

    Args:
        parquet_src: Raven output parquet file

    Returns:
        Mean Annual Flow scalar for each site
    """
    result_df = mean_annual_flow(parquet_src)

    return result_df.to_dict(orient="records")
