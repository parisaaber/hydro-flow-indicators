import logging
from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import pandas as pd
import numpy as np
import duckdb
from scipy.stats import gumbel_r

app = FastAPI(title="Raven API", version="0.1")

logging.basicConfig(level=logging.INFO)


def mean_annual_flow(parquet_path: str) -> pd.Series:
    """
    Calculate mean annual flow for each site.
    """
    try:
        con = duckdb.connect()

        df = con.execute(
            f"""
            SELECT avg(value), site
            FROM parquet_scan('{parquet_path}')
            GROUP BY site
        """
        ).fetchdf()

        con.close()
    except Exception as e:
        logging.error(f"Failed to read parquet file: {e}")
        raise RuntimeError(f"Failed to read parquet file: {e}")

    return df.rename("mean_annual_flow")


def mean_aug_sep_flow(df: pd.DataFrame) -> pd.Series:
    """
    Calculate mean flow for August and September for each site.
    """
    aug_sep = df[df["month"].isin([8, 9])]
    return (
        aug_sep.groupby(["site", "water_year"])["value"]
        .mean()
        .groupby("site")
        .mean()
        .rename("mean_aug_sep_flow")
    )


def peak_flow_timing(df: pd.DataFrame) -> pd.Series:
    """
    Calculate average peak flow timing (day of year) per site.
    """
    df["doy"] = df["date"].dt.dayofyear
    peak_days = df.groupby(["site", "water_year"])[["value", "doy"]].apply(
        lambda x: x.loc[x["value"].idxmax(), "doy"]
    )
    return peak_days.groupby("site").mean().rename("peak_flow_timing")


def days_below_efn(df: pd.DataFrame, EFN_threshold: float) -> pd.Series:
    """
    Calculate number of days below EFN threshold per site.
    """
    mean_annual = (
        df.groupby(["site", "water_year"])["value"].mean().groupby("site").mean()
    )
    threshold = mean_annual * EFN_threshold

    def count_days(group: pd.DataFrame) -> int:
        site = group.name[0]
        return (group["value"] < threshold[site]).sum()

    counts = df.groupby(["site", "water_year"]).apply(count_days)
    return counts.groupby("site").mean().rename("days_below_efn")


def annual_peaks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get annual peak flows for each site and water year.
    """
    return df.groupby(["site", "water_year"])["value"].max().reset_index(name="value")


def fit_ffa(
    peaks_df: pd.DataFrame, dist: str = "gumbel", return_periods: list[int] = [2, 20]
) -> pd.DataFrame:
    """
    Fit Flood Frequency Analysis (FFA) using Gumbel distribution.

    Args:
        peaks_df: DataFrame with 'site', 'water_year', 'value'
        dist: Distribution name (only 'gumbel' supported)
        return_periods: List of return periods (e.g., [2, 20])

    Returns:
        DataFrame with flood quantiles per site.
    """
    result = []
    for site, group in peaks_df.groupby("site"):
        values = group["value"].dropna()
        if len(values) < 2:
            continue
        if dist == "gumbel":
            loc, scale = gumbel_r.fit(values)
            rp_values = {
                f"Q{rp}": gumbel_r.ppf(1 - 1 / rp, loc=loc, scale=scale)
                for rp in return_periods
            }
            rp_values["site"] = site
            result.append(rp_values)
    return pd.DataFrame(result)


def peak_flows(df: pd.DataFrame) -> pd.Series:
    """
    Calculate mean annual peak flow per site.
    """
    return (
        df.groupby(["site", "water_year"])["value"]
        .max()
        .groupby("site")
        .mean()
        .rename("mean_annual_peak")
    )


def calculate_all_indicators(
    parquet_path: str, EFN_threshold: float = 0.2, break_point: Optional[int] = None
) -> pd.DataFrame:
    """
    Calculate all indicators from Raven output stored as Parquet.

    Args:
        parquet_path: Path to Parquet file.
        EFN_threshold: Threshold for EFN indicator.
        break_point: Water year to split subperiods.

    Returns:
        DataFrame with calculated indicators for each subperiod and site.
    """
    logging.info(f"Reading parquet file from: {parquet_path}")
    try:
        con = duckdb.connect()
        df = con.execute(
            f"""
            SELECT date, value, site
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
        """
        ).fetchdf()
    except Exception as e:
        logging.error(f"Failed to read parquet file: {e}")
        raise RuntimeError(f"Failed to read parquet file: {e}")

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.month
        df["year"] = df["date"].dt.year
        df["water_year"] = np.where(df["month"] >= 10, df["year"] + 1, df["year"])

        if break_point:
            df["subperiod"] = np.where(
                df["water_year"] <= break_point,
                f"before_{break_point}",
                f"after_{break_point}",
            )
        else:
            df["subperiod"] = "full_period"

        results = []
        for period in df["subperiod"].unique():
            sub_df = df[df["subperiod"] == period]

            indicators = pd.concat(
                [
                    mean_annual_flow(sub_df),
                    mean_aug_sep_flow(sub_df),
                    peak_flow_timing(sub_df),
                    days_below_efn(sub_df, EFN_threshold),
                    peak_flows(sub_df),
                ],
                axis=1,
            )

            peaks_df = annual_peaks(sub_df)
            ffa = fit_ffa(peaks_df, return_periods=[2, 20]).set_index("site")

            all_indicators = indicators.join(ffa, how="left")
            all_indicators["subperiod"] = period
            results.append(all_indicators.reset_index())

        final_df = pd.concat(results, ignore_index=True)
        logging.info(f"Calculated indicators for {len(final_df)} records")
        return final_df

    except Exception as e:
        logging.error(f"Error in calculating indicators: {e}")
        raise RuntimeError(f"Error in calculating indicators: {e}")
