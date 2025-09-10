from typing import Optional, List
import pandas as pd
import numpy as np
import duckdb
import warnings
from .utils import (
    detect_outliers_duckdb,
    fit_distribution,
    select_best_distribution,
    calculate_return_period_values,
    get_site_statistics_duckdb,
)


def mean_annual_flow(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    temporal_resolution: str = "overall"
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
            WHERE value IS NOT NULL
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
                WHERE value IS NOT NULL
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
    temporal_resolution: str = "overall"
) -> pd.DataFrame:
    """
    Calculate the mean August–September flow for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'mean_aug_sep_flow' columns.
    """
    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f"AND site IN {sites_tuple}"
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
                {sites_filter}
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
                {sites_filter}
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
    temporal_resolution: str = "overall"
) -> pd.DataFrame:
    """
    Estimate the average day of year of annual peak flow for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'peak_flow_timing' columns.
    """
    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f"AND site IN {sites_tuple}"
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"

    if temporal_resolution == "annual":
        query = f"""
            WITH ranked_flows AS (
                SELECT site, water_year, value,
                EXTRACT(doy FROM date) AS doy,
                ROW_NUMBER() OVER (PARTITION BY site,
                water_year ORDER BY value DESC) AS rn
                FROM parquet_scan('{parquet_path}')
                WHERE value IS NOT NULL
                {sites_filter}
                {date_filter}
            )
            SELECT site, water_year, doy AS peak_flow_timing
            FROM ranked_flows
            WHERE rn = 1
            ORDER BY site, water_year
        """
    else:
        query = f"""
            WITH ranked_flows AS (
                SELECT site, water_year, value,
                EXTRACT(doy FROM date) AS doy,
                ROW_NUMBER() OVER (PARTITION BY site,
                water_year ORDER BY value DESC) AS rn
                FROM parquet_scan('{parquet_path}')
                WHERE value IS NOT NULL
                {sites_filter}
                {date_filter}
            ),
            annual_peaks AS (
                SELECT site, water_year, doy
                FROM ranked_flows
                WHERE rn = 1
            )
            SELECT site, AVG(doy) AS peak_flow_timing
            FROM annual_peaks
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
    temporal_resolution: str = "overall"
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
    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f"AND p.site IN {sites_tuple}"
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
            {sites_filter}
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
    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f"AND site IN {sites_tuple}"

    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM parquet_scan('{parquet_path}')
        WHERE value IS NOT NULL
        {sites_filter}
        {date_filter}
        GROUP BY site, water_year
        ORDER BY site, water_year
        """
    return con.execute(query).fetchdf()


def fit_ffa(
    peaks_df: pd.DataFrame,
    dist: str = "auto",
    return_periods: List[int] = [2, 20],
    sites: Optional[List[str]] = None,
    remove_outliers: bool = False,
    outlier_method: str = "iqr",
    outlier_threshold: float = 1.5,
    available_distributions: List[str] =
    ["gumbel", "genextreme", "normal", "lognormal", "gamma", "logpearson3"],
    selection_criteria: str = "aic",
    min_years: int = 5,
    use_duckdb_stats: bool = True,
    debug: bool = False  # Add debug option
) -> pd.DataFrame:
    """
    Enhanced Flood Frequency Analysis with DuckDB
    integration, multiple distributions,
    outlier detection, and automatic best-fit selection.

    Args:
        peaks_df: DataFrame with 'site', 'water_year', 'annual_peak'.
        dist: Distribution name or 'auto' for automatic selection.
        return_periods: List of return periods to estimate (e.g., [2, 20]).
        sites: Optional list of sites to analyze.
        remove_outliers: Whether to remove outliers before fitting.
        outlier_method: Method for outlier detection ('iqr', 'zscore',
        'modified_zscore').
        outlier_threshold: Threshold for outlier detection.
        available_distributions: List of distributions to consider
        for auto selection.
        selection_criteria: Criteria for automatic distribution selection
        ('aic', 'bic', 'ks', 'rmse').
        min_years: Minimum number of years required for analysis.
        use_duckdb_stats: Whether to use DuckDB for statistical calculations.
        debug: Whether to print debug information.

    Returns:
        DataFrame with 'site', return period discharge values,
        'best_distribution',
        and 'outliers_removed' columns.
    """
    # Initialize DuckDB connection
    conn = duckdb.connect(':memory:')
    try:
        # Register DataFrame with DuckDB
        conn.register('peaks_data', peaks_df)
        # Filter sites if specified
        if sites:
            placeholders = ", ".join(["?"] * len(sites))
            filter_query = (
                f"SELECT * FROM peaks_data WHERE site IN ({placeholders})"
                )
            working_df = conn.execute(filter_query, sites).df()
        else:
            working_df = peaks_df.copy()
        # Get site statistics using DuckDB
        if use_duckdb_stats:
            site_stats = get_site_statistics_duckdb(conn, 'peaks_data')
            print("Site Statistics:")
            print(site_stats)
        # Re-register the working DataFrame
        conn.register('working_data', working_df)
        # Get sites with sufficient data using DuckDB
        sites_query = f"""
        SELECT site, COUNT(annual_peak) as n_years
        FROM working_data
        WHERE annual_peak IS NOT NULL
        GROUP BY site
        HAVING COUNT(annual_peak) >= {min_years}
        ORDER BY site
        """
        valid_sites = conn.execute(sites_query).df()
        if valid_sites.empty:
            warnings.warn(
                f"No sites have sufficient data"
                f"(minimum {min_years} years)")
            return pd.DataFrame()
        results = []
        for _, site_row in valid_sites.iterrows():
            site = site_row['site']
            # Get data for this site using DuckDB
            site_query = f"""
            SELECT annual_peak, water_year
            FROM working_data
            WHERE site = '{site}' AND annual_peak IS NOT NULL
            ORDER BY water_year
            """
            site_data = conn.execute(site_query).df()
            values = site_data['annual_peak'].values
            if debug:
                print(f"\nProcessing site: {site}")
                print(
                    f"Data summary: min={values.min():.2f}, "
                    f"max={values.max():.2f}, mean={values.mean():.2f}, "
                    f"std={values.std():.2f}")
                print(f"Data points: {len(values)}")
            # Remove outliers if requested using DuckDB
            outliers_removed = 0
            if remove_outliers:
                # Create temporary table for this site
                conn.register('temp_site_data', site_data)
                # Get outlier detection query
                outlier_query = detect_outliers_duckdb(
                    conn,
                    'temp_site_data',
                    'annual_peak',
                    outlier_method,
                    outlier_threshold
                )
                # Execute outlier detection
                outlier_results = conn.execute(outlier_query).df()
                # Count and remove outliers
                outliers_removed = outlier_results['is_outlier'].sum()
                clean_data = outlier_results[~outlier_results['is_outlier']]
                values = clean_data['annual_peak'].values
                if debug:
                    print(f"Outliers removed: {outliers_removed}")
                    print(f"Clean data points: {len(values)}")
                # Check if we still have enough data after outlier removal
                if len(values) < min_years:
                    warnings.warn(
                        f"Site {site}: Insufficient data after outlier removal"
                        )
                    continue

            try:
                # Determine distribution to use
                if dist == "auto":
                    if debug:
                        print(
                            f"Trying distributions: {available_distributions}")
                    best_dist, best_params = select_best_distribution(
                        values, available_distributions, selection_criteria
                    )
                    if debug:
                        print(f"Best distribution selected: {best_dist}")
                else:
                    best_dist = dist
                    best_params, gof_stats = fit_distribution(values, dist)
                    if best_params is None:
                        if debug:
                            print(f"Failed to fit {dist}: {gof_stats}")
                        warnings.warn(
                            f"Site {site}:"
                            f"Could not fit {dist} distribution - "
                            f"{gof_stats.get('error', 'Unknown error')}"
                            )
                        continue

                # Calculate return period values
                if any(rp > len(values) for rp in return_periods):
                    warnings.warn(
                        f"Site {site}: Some return periods"
                        "{return_periods} exceed "
                        f"data years ({len(values)})."
                        "Estimates may be unreliable."
                    )
                rp_values = calculate_return_period_values(
                    best_dist, best_params, return_periods
                    )
                if debug:
                    print(f"Return period values: {rp_values}")

                # Get additional statistics using DuckDB
                site_stats_query = f"""
                SELECT
                    AVG(annual_peak) as mean_flow,
                    STDDEV(annual_peak) as std_flow,
                    MIN(annual_peak) as min_flow,
                    MAX(annual_peak) as max_flow
                FROM working_data
                WHERE site = '{site}' AND annual_peak IS NOT NULL
                """
                stats_result = conn.execute(site_stats_query).df().iloc[0]

                # Compile results
                site_result = {
                    "site": site,
                    "best_distribution": best_dist,
                    "outliers_removed": int(outliers_removed),
                    "data_years": len(values),
                    "mean_flow": float(stats_result['mean_flow']),
                    "std_flow": float(stats_result['std_flow']),
                    "min_flow": float(stats_result['min_flow']),
                    "max_flow": float(stats_result['max_flow'])
                }
                site_result.update(rp_values)
                results.append(site_result)
                if debug:
                    print(f"Successfully processed site {site}")

            except Exception as e:
                if debug:
                    print(f"Error processing site {site}: {str(e)}")
                warnings.warn(f"Site {site}: Error in analysis - {str(e)}")
                continue

        if not results:
            warnings.warn("No sites could be successfully analyzed")
            return pd.DataFrame()

        # Convert results to DataFrame and perform final operations with DuckDB
        results_df = pd.DataFrame(results)
        # Register results for potential additional DuckDB operations
        conn.register('results_data', results_df)
        # Add ranking based on mean flow using DuckDB
        ranking_query = """
        SELECT *,
            RANK() OVER (ORDER BY mean_flow DESC) as flow_rank
        FROM results_data
        ORDER BY flow_rank
        """
        final_results = conn.execute(ranking_query).df()
        return final_results
    finally:
        # Close DuckDB connection
        conn.close()


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
    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f"AND site IN {sites_tuple}"

    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
        WITH annual_peaks AS (
            SELECT site, water_year, MAX(value) AS annual_peak
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
            {sites_filter}
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
    base_filter = "WHERE value IS NOT NULL"
    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f" AND site IN {sites_tuple}"

    date_filter = ""
    if start_date and end_date:
        date_filter = f" AND date BETWEEN '{start_date}' AND '{end_date}'"

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
    {base_filter}
    {sites_filter}
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

        sites_filter = ""
        if sites:
            sites_tuple = tuple(sites)
            sites_filter = (
                f"AND site IN {sites_tuple}"
                if date_filter else f"WHERE site IN {sites_tuple}"
            )

        query = f"""
            SELECT date, site
            FROM parquet_scan('{parquet_path}')
            {date_filter}
            {sites_filter}
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
                f"after_{break_point}"
            )
            subperiods = df_dates["subperiod"].unique()
        else:
            subperiods = ["full_period"]

        results = []

        for period in subperiods:
            if period == "full_period":
                sub_start = start_date
                sub_end = end_date
            elif break_point is not None:
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
                        end_date=sub_end
                    ).set_index("site"),
                    mean_aug_sep_flow(
                        con,
                        parquet_path,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end
                    ).set_index("site"),
                    peak_flow_timing(
                        con,
                        parquet_path,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end
                    ).set_index("site"),
                    days_below_efn(
                        con,
                        parquet_path,
                        EFN_threshold,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end
                    ).set_index("site"),
                    peak_flows(
                        con,
                        parquet_path,
                        sites=sites,
                        start_date=sub_start,
                        end_date=sub_end
                    ).set_index("site"),
                ],
                axis=1
            )

            peaks_df = annual_peaks(
                con,
                parquet_path,
                sites=sites,
                start_date=sub_start,
                end_date=sub_end
            )
            ffa = fit_ffa(
                peaks_df,
                return_periods=[2, 20],
                sites=sites
            ).set_index("site")

            all_indicators = indicators.join(ffa, how="left")
            all_indicators["subperiod"] = period
            results.append(all_indicators.reset_index())

        con.close()
        final_df = pd.concat(results, ignore_index=True)
        final_df.replace([np.nan, np.inf, -np.inf], None, inplace=True)
        return final_df

    except Exception as e:
        raise RuntimeError(f"Error in calculating indicators: {e}")


def aggregate_flows(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,  # Format: "YYYY-MM-DD"
    end_date: Optional[str] = None,    # Format: "YYYY-MM-DD"
    temporal_resolution: str = "daily"  # "weekly","monthly", "seasonal"
) -> pd.DataFrame:

    sites_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        sites_filter = f"AND site IN {sites_tuple}"

    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"

    if temporal_resolution == "daily":
        group_expr = "date"
    elif temporal_resolution == "weekly":
        group_expr = "strftime('%Y-%W', date)"
    elif temporal_resolution == "monthly":
        group_expr = "strftime('%Y-%m', date)"
    elif temporal_resolution == "seasonal":
        # Define season as "YYYY-Season"
        group_expr = """
            strftime('%Y', date) || '-' ||
            CASE
                WHEN CAST(strftime('%m', date) AS INTEGER)
                IN (12, 1, 2) THEN 'Winter'
                WHEN CAST(strftime('%m', date) AS INTEGER)
                BETWEEN 3 AND 5 THEN 'Spring'
                WHEN CAST(strftime('%m', date) AS INTEGER)
                BETWEEN 6 AND 8 THEN 'Summer'
                WHEN CAST(strftime('%m', date) AS INTEGER)
                BETWEEN 9 AND 11 THEN 'Fall'
            END
        """
    else:
        raise ValueError(
            "Invalid temporal_resolution: choose from"
            "daily, weekly, monthly, seasonal"
                        )

    query = f"""
        SELECT
            site,
            {group_expr} AS period,
            AVG(value) AS mean_flow
        FROM parquet_scan('{parquet_path}')
        WHERE value IS NOT NULL
        {sites_filter}
        {date_filter}
        GROUP BY site, period
        ORDER BY site, period
    """

    df = con.execute(query).fetchdf()
    return df.rename(columns={"period": "time_period"})
