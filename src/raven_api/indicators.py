from fastapi import FastAPI, Query
from typing import Optional, List
import pandas as pd
import numpy as np
import duckdb
from scipy.stats import gumbel_r

app = FastAPI(title="Raven API", version="0.1")


def common_query_params(
    parquet_src: str = Query(..., description="Full path to Parquet"),
    sites: Optional[List[str]] = Query(
        None, description="List of site IDs", example=["sub11004314 [m3/s]"]
    ),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    temporal_resolution: str = Query("overall", description="overall|annual"),
    efn_threshold: float = Query(0.2, description="EFN threshold fraction"),
    break_point: Optional[int] = Query(
        None, description="Water year to split subperiods"
    ),
):
    return {
        "parquet_src": parquet_src,
        "sites": sites,
        "start_date": start_date,
        "end_date": end_date,
        "temporal_resolution": temporal_resolution,
        "efn_threshold": efn_threshold,
        "break_point": break_point,
    }


def mean_annual_flow(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    temporal_resolution: str = "overall",
) -> pd.DataFrame:
    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f"AND site IN {sites_tuple}"

    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
    if temporal_resolution == "annual":
        # Return mean flow per site per water year
        query = f"""
            SELECT site, water_year, AVG(value) AS mean_annual_flow
            FROM parquet_scan('{parquet_path}')
            WHERE 1=1
            {sites_filter}
            {date_filter}
            GROUP BY site, water_year
            ORDER BY site, water_year
        """
    else:
        # Return one average value per site over entire period
        query = f"""
            WITH mean_1 AS (
                SELECT AVG(value) AS maf_per_year, site, water_year
                FROM parquet_scan('{parquet_path}')
                WHERE 1=1
                {sites_filter}
                {date_filter}
                GROUP BY site, water_year
            )
            SELECT site, AVG(maf_per_year) AS mean_annual_flow
            FROM mean_1
            GROUP BY site
            ORDER BY site
        """
    return con.execute(query).fetchdf()


def mean_aug_sep_flow(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    temporal_resolution: str = "overall",
) -> pd.DataFrame:
    """
    Calculate the mean August–September flow for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'mean_aug_sep_flow' columns.
    """
    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
    if temporal_resolution == "annual":
        # Return mean_aug_sep_flow per site per water year
        query = f"""
            WITH aug_sep AS (
                SELECT site, water_year, AVG(value) AS avg_aug_sep_flow
                FROM parquet_scan('{parquet_path}')
                WHERE EXTRACT(month FROM date) IN (8, 9)
                {site_filter}
                {date_filter}
                GROUP BY site, water_year
            )
            SELECT site, water_year, avg_aug_sep_flow AS mean_aug_sep_flow
            FROM aug_sep
            ORDER BY site, water_year
        """
    else:
        query = f"""
            WITH aug_sep AS (
                SELECT site, water_year, AVG(value) AS avg_aug_sep_flow
                FROM parquet_scan('{parquet_path}')
                WHERE EXTRACT(month FROM date) IN (8, 9)
                {site_filter}
                {date_filter}
                GROUP BY site, water_year
            )
            SELECT site, AVG(avg_aug_sep_flow) AS mean_aug_sep_flow
            FROM aug_sep
            GROUP BY site
            ORDER BY site
        """
    return con.execute(query).fetchdf()


def peak_flow_timing(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    temporal_resolution: str = "overall",
) -> pd.DataFrame:
    """
    Estimate the average day of year of annual peak flow for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'peak_flow_timing' columns.
    """
    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"

    if temporal_resolution == "annual":
        query = f"""
            WITH daily AS (
                SELECT site, water_year, value, EXTRACT(doy FROM date) AS doy
                FROM parquet_scan('{parquet_path}')
                WHERE value IS NOT NULL
                {site_filter}
                {date_filter}
            ),
            peaks AS (
                SELECT site, water_year,
                FIRST_VALUE(doy) OVER (PARTITION BY site,
                water_year ORDER BY value DESC) AS peak_doy
                FROM daily
            )
            SELECT site, water_year, peak_doy AS peak_flow_timing
            FROM peaks
            GROUP BY site, water_year, peak_doy
            ORDER BY site, water_year
        """
    else:
        query = f"""
            WITH daily AS (
                SELECT site, water_year, value, EXTRACT(doy FROM date) AS doy
                FROM parquet_scan('{parquet_path}')
                WHERE value IS NOT NULL
                {site_filter}
                {date_filter}
            ),
            peaks AS (
                SELECT site, water_year,
                FIRST_VALUE(doy) OVER (PARTITION BY site,
                water_year ORDER BY value DESC) AS peak_doy
                FROM daily
            )
            SELECT site, AVG(peak_doy) AS peak_flow_timing
            FROM peaks
            GROUP BY site
            ORDER BY site
        """
    return con.execute(query).fetchdf()


def days_below_efn(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    EFN_threshold: float,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    temporal_resolution: str = "overall",
) -> pd.DataFrame:
    """
    Calculate the average number of days per year
    below the Environmental Flow Needs (EFN) threshold.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.
        EFN_threshold: Proportion (e.g., 0.2) of
        mean annual flow used as the threshold.

    Returns:
        DataFrame with 'site' and 'days_below_efn' columns.
    """
    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND p.site IN {sites_tuple}"
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND p.date BETWEEN '{start_date}' AND '{end_date}'"
    query = f"""
        WITH mean_annual AS (
            SELECT site, water_year, AVG(value) AS maf_year
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
            GROUP BY site, water_year
        ),
        mean_annual_site AS (
            SELECT site, AVG(maf_year) AS maf_site
            FROM mean_annual
            GROUP BY site
        ),
        daily_with_threshold AS (
            SELECT p.site, p.water_year, p.value,
                   m.maf_site, m.maf_site * {EFN_threshold} AS threshold
            FROM parquet_scan('{parquet_path}') p
            JOIN mean_annual_site m ON p.site = m.site
            WHERE value IS NOT NULL
            {site_filter}
            {date_filter}
        )
    """

    if temporal_resolution == "annual":
        query += """
        ,
        days_below AS (
            SELECT site, water_year,
            COUNT(*) FILTER (WHERE value < threshold) AS days_below_efn
            FROM daily_with_threshold
            GROUP BY site, water_year
        )
        SELECT site, water_year, days_below_efn
        FROM days_below
        ORDER BY site, water_year
        """
    else:
        query += """
        ,
        days_below AS (
            SELECT site, water_year,
            COUNT(*) FILTER (WHERE value < threshold) AS days_below_efn
            FROM daily_with_threshold
            GROUP BY site, water_year
        )
        SELECT site, AVG(days_below_efn) AS days_below_efn
        FROM days_below
        GROUP BY site
        ORDER BY site
        """
    return con.execute(query).fetchdf()


def annual_peaks(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract annual peak flow for each site and water year.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site', 'water_year', and 'annual_peak' columns.
    """
    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"

    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM parquet_scan('{parquet_path}')
        WHERE value IS NOT NULL
        {site_filter}
        {date_filter}
        GROUP BY site, water_year
        ORDER BY site, water_year
        """
    return con.execute(query).fetchdf()


def fit_ffa(
    peaks_df: pd.DataFrame,
    dist: str = "gumbel",
    return_periods: list[int] = [2, 20],
    sites: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Fit Flood Frequency Analysis using Gumbel distribution
    for specified return periods.

    Args:
        peaks_df: DataFrame with 'site', 'water_year', 'annual_peak'.
        dist: Distribution name (currently only 'gumbel' is supported).
        return_periods: List of return periods to estimate (e.g., [2, 20]).

    Returns:
        DataFrame with 'site' and return period
        discharge values (e.g., 'Q2', 'Q20').
    """
    if sites:
        peaks_df = peaks_df[peaks_df["site"].isin(sites)]
    result = []
    for site, group in peaks_df.groupby("site"):
        values = group["annual_peak"].dropna()
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


def peak_flows(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calculate mean annual peak flow for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'mean_annual_peak' columns.
    """
    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"

    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
        WITH annual_peaks AS (
            SELECT site, water_year, MAX(value) AS annual_peak
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
            {site_filter}
            {date_filter}
            GROUP BY site, water_year
        )
        SELECT site, AVG(annual_peak) AS mean_annual_peak
        FROM annual_peaks
        GROUP BY site
        ORDER BY site
        """
    return con.execute(query).fetchdf()


def weekly_flow_exceedance(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"WHERE site IN {sites_tuple}"
    else:
        site_filter = ""

    date_filter = ""
    if start_date and end_date:
        if site_filter:
            date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
        else:
            date_filter = f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
    SELECT
        site,
        CAST(strftime('%W', date) AS INTEGER) AS week,
        quantile_cont(value, 0.90) AS p10,
        quantile_cont(value, 0.80) AS p20,
        quantile_cont(value, 0.70) AS p30,
        quantile_cont(value, 0.60) AS p40,
        quantile_cont(value, 0.50) AS p50,
        quantile_cont(value, 0.40) AS p60,
        quantile_cont(value, 0.30) AS p70,
        quantile_cont(value, 0.20) AS p80,
        quantile_cont(value, 0.05) AS p95
    FROM parquet_scan('{parquet_path}')
    {site_filter}
    {date_filter}
    GROUP BY
        site,
        CAST(strftime('%W', date) AS INTEGER)
    ORDER BY site, week
    """
    return con.execute(query).fetchdf()


def calculate_all_indicators(
    parquet_path: str,
    EFN_threshold: float = 0.2,
    break_point: Optional[int] = None,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calculate a set of hydrologic indicators for one or more subperiods.

    Indicators include:
    - Mean annual flow
    - August-September mean flow
    - Peak flow timing (DOY)
    - Number of days below EFN threshold
    - Mean annual peak flow
    - Flood quantiles (Q2, Q20)

    Args:
        parquet_path: Path to Raven-generated Parquet file.
        EFN_threshold: Proportion of MAF used for
        low flow threshold (e.g., 0.2).
        break_point: Water year to split subperiods
        (e.g., 2000), or None for full period.
        sites: List of site IDs to filter (optional).
        start_date: Start date for filtering data (optional).
        end_date: End date for filtering data (optional).

    Returns:
        DataFrame containing calculated indicators for each site and subperiod.
    """
    try:
        con = duckdb.connect()

        # Build date and site filters
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"

        site_filter = ""
        if sites:
            sites_tuple = tuple(sites)
            site_filter = (
                f"AND site IN {sites_tuple}"
                if date_filter
                else f"WHERE site IN {sites_tuple}"
            )

        query = f"""
            SELECT date, site
            FROM parquet_scan('{parquet_path}')
            {date_filter}
            {site_filter}
        """
        df_dates = con.execute(query).fetchdf()
        df_dates["date"] = pd.to_datetime(df_dates["date"])
        df_dates["month"] = df_dates["date"].dt.month
        df_dates["year"] = df_dates["date"].dt.year
        df_dates["water_year"] = np.where(
            df_dates["month"] >= 10,
            df_dates["year"] + 1,
            df_dates["year"],
        )

        # Define subperiods
        if break_point:
            df_dates["subperiod"] = np.where(
                df_dates["water_year"] <= break_point,
                f"before_{break_point}",
                f"after_{break_point}",
            )
            subperiods = df_dates["subperiod"].unique()
        else:
            subperiods = ["full_period"]

        results = []

        for period in subperiods:
            if period == "full_period":
                sub_start = start_date
                sub_end = end_date
            else:
                if "before" in period:
                    sub_start = start_date
                    sub_end = f"{break_point}-09-30"
                else:
                    sub_start = f"{break_point + 1}-10-01"
                    sub_end = end_date

            indicators = pd.concat(
                [
                    mean_annual_flow(
                        con,
                        parquet_path,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end,
                    ).set_index("site"),
                    mean_aug_sep_flow(
                        con,
                        parquet_path,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end,
                    ).set_index("site"),
                    peak_flow_timing(
                        con,
                        parquet_path,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end,
                    ).set_index("site"),
                    days_below_efn(
                        con,
                        parquet_path,
                        EFN_threshold,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end,
                    ).set_index("site"),
                    peak_flows(
                        con,
                        parquet_path,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end,
                    ).set_index("site"),
                ],
                axis=1,
            )

            peaks_df = annual_peaks(
                con, parquet_path, sites=sites, start_date=sub_start, end_date=sub_end
            )
            ffa = fit_ffa(peaks_df, return_periods=[2, 20], sites=sites).set_index(
                "site"
            )

            all_indicators = indicators.join(ffa, how="left")
            all_indicators["subperiod"] = period
            results.append(all_indicators.reset_index())

        con.close()
        final_df = pd.concat(results, ignore_index=True)
        final_df.replace([np.nan, np.inf, -np.inf], None, inplace=True)
        return final_df

    except Exception as e:
        raise RuntimeError(f"Error in calculating indicators: {e}")
