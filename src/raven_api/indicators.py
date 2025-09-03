from fastapi import FastAPI
from typing import Optional, List, Dict, Tuple
import pandas as pd
import numpy as np
import scipy.stats as stats
import duckdb
import warnings
from scipy.stats import (
    gumbel_r, norm, lognorm, genextreme, gamma, weibull_min, pearson3
    )


app = FastAPI(title="Raven API", version="0.1")


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


def detect_outliers_duckdb(
    conn: duckdb.DuckDBPyConnection, table_name: str,
    column: str, method: str = "iqr", threshold: float = 1.5
) -> str:
    """
    Generate SQL query for outlier detection using DuckDB.
    Args:
        conn: DuckDB connection
        table_name: Name of the table
        column: Column name to check for outliers
        method: Method for outlier detection
        ('iqr', 'zscore', 'modified_zscore')
        threshold: Threshold value for outlier detection
    Returns:
        SQL query string for outlier detection
    """
    if method == "iqr":
        query = f"""
        WITH quartiles AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) AS Q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) AS Q3
            FROM {table_name}
        ),
        bounds AS (
            SELECT
                Q1 - {threshold} * (Q3 - Q1) AS lower_bound,
                Q3 + {threshold} * (Q3 - Q1) AS upper_bound
            FROM quartiles
        )
        SELECT *,
            CASE WHEN {column} < bounds.lower_bound OR {column} >
            bounds.upper_bound
            THEN true ELSE false END AS is_outlier
        FROM {table_name}, bounds
        """
    elif method == "zscore":
        query = f"""
        WITH stats AS (
            SELECT
                AVG({column}) AS mean_val,
                STDDEV({column}) AS std_val
            FROM {table_name}
        )
        SELECT *,
            CASE WHEN ABS(
                ({column} - stats.mean_val) / stats.std_val) > {threshold}
            THEN true ELSE false END AS is_outlier
        FROM {table_name}, stats
        """
    elif method == "modified_zscore":
        query = f"""
        WITH median_stats AS (
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP
                (ORDER BY {column}) AS median_val
            FROM {table_name}
        ),
        mad_stats AS (
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY ABS({column} - median_stats.median_val)) AS mad_val
            FROM {table_name}, median_stats
        )
        SELECT *,
            CASE WHEN ABS(0.6745 * (
                {column} - median_stats.median_val) /
                NULLIF(mad_stats.mad_val, 0)
            ) > {threshold}
            THEN true ELSE false END AS is_outlier
        FROM {table_name}, median_stats, mad_stats
        """
    else:
        raise ValueError(
            "Method must be 'iqr', 'zscore', or 'modified_zscore'"
        )
    return query


def calculate_goodness_of_fit(
    data: np.ndarray,
    distribution: str,
    params: tuple
) -> Dict[str, float]:
    """
    Calculate goodness-of-fit statistics for a distribution.
    Args:
        data: Observed data
        distribution: Distribution name
        params: Distribution parameters
    Returns:
        Dictionary with goodness-of-fit statistics
    """
    try:
        # Map name -> scipy dist object
        if distribution == "gumbel":
            dist_obj = gumbel_r
        elif distribution == "normal":
            dist_obj = norm
        elif distribution == "lognormal":
            dist_obj = lognorm
        elif distribution == "genextreme":
            dist_obj = genextreme
        elif distribution == "gamma":
            dist_obj = gamma
        elif distribution == "weibull":
            dist_obj = weibull_min
        elif distribution == "logpearson3":
            dist_obj = pearson3
        else:
            raise ValueError(f"Distribution '{distribution}' not supported")

        # --- KS + loglik (single computation; no overwrite) ---
        if distribution == "logpearson3":
            # Fit/GOF done in log-space for Log-Pearson III
            log_data = np.log(data)
            ks_stat, ks_pvalue = stats.kstest(
                log_data, lambda x: pearson3.cdf(x, *params)
            )
            log_likelihood = np.sum(pearson3.logpdf(log_data, *params))
        else:
            ks_stat, ks_pvalue = stats.kstest(
                data, lambda x: dist_obj.cdf(x, *params)
            )
            log_likelihood = np.sum(dist_obj.logpdf(data, *params))

        if not np.isfinite(log_likelihood):
            return {"error": "Invalid log likelihood"}

        # --- AIC/BIC ---
        k = len(params)
        n = len(data)
        aic = 2 * k - 2 * log_likelihood
        bic = k * np.log(n) - 2 * log_likelihood

        # --- RMSE for QQ ---
        # Compare in *data scale* for interpretability.
        sorted_data = np.sort(data)
        n_points = len(data)
        probabilities = (np.arange(1, n_points + 1) - 0.5) / n_points

        if distribution == "logpearson3":
            # Theoretical in log-space then exponentiate to data scale
            theo_q_log = pearson3.ppf(probabilities, *params)
            theoretical_quantiles = np.exp(theo_q_log)
        else:
            theoretical_quantiles = dist_obj.ppf(probabilities, *params)

        rmse = np.sqrt(np.mean((theoretical_quantiles - sorted_data) ** 2))

        return {
            "ks_stat": ks_stat,
            "ks_pvalue": ks_pvalue,
            "aic": aic,
            "bic": bic,
            "rmse": rmse,
            "log_likelihood": log_likelihood,
        }
    except Exception as e:
        return {"error": f"GOF calculation failed: {str(e)}"}


def fit_distribution(
    data: np.ndarray,
    distribution: str
) -> Tuple[tuple, Dict[str, float]]:
    """
    Fit a distribution to data and return parameters
    and goodness-of-fit statistics.
    Args:
        data: Input data
        distribution: Distribution name
    Returns:
        Tuple of (parameters, goodness_of_fit_stats)
    """
    try:
        # Ensure data is valid
        if len(data) < 3:
            return None, {"error": "Insufficient data points"}
        if np.any(~np.isfinite(data)):
            return None, {"error": "Data contains non-finite values"}
        # Fit distribution with error handling
        if distribution == "gumbel":
            params = gumbel_r.fit(data)
        elif distribution == "normal":
            params = norm.fit(data)
        elif distribution == "lognormal":
            # For lognormal, data must be positive
            if np.any(data <= 0):
                return None, {"error": "Lognormal requires positive data"}
            # Use method of moments for better stability
            try:
                params = lognorm.fit(data, floc=0)
            except (ValueError, RuntimeError, np.linalg.LinAlgError):
                # Fallback method
                log_data = np.log(data)
                mu = np.mean(log_data)
                sigma = np.std(log_data)
                params = (sigma, 0, np.exp(mu))
        elif distribution == "genextreme":
            try:
                params = genextreme.fit(data)
            except (ValueError, RuntimeError, np.linalg.LinAlgError):
                # Fallback to method of moments
                mean_val = np.mean(data)
                std_val = np.std(data)
                # Simple approximation
                params = (0.1, mean_val - 0.45 * std_val, 0.78 * std_val)
        elif distribution == "gamma":
            if np.any(data <= 0):
                return None, {"error": "Gamma requires positive data"}
            try:
                params = gamma.fit(data, floc=0)
            except (ValueError, RuntimeError, np.linalg.LinAlgError):
                # Fallback method of moments
                mean_val = np.mean(data)
                var_val = np.var(data)
                scale = var_val / mean_val
                shape = mean_val / scale
                params = (shape, 0, scale)
        elif distribution == "weibull":
            if np.any(data <= 0):
                return None, {"error": "Weibull requires positive data"}
            params = weibull_min.fit(data, floc=0)
        elif distribution == "logpearson3":
            # Log-Pearson III: fit Pearson III to log-transformed data
            if np.any(data <= 0):
                return None, {
                    "error": "Log-Pearson III requires positive data"}
            log_data = np.log(data)
            params = pearson3.fit(log_data)
        else:
            raise ValueError(f"Distribution '{distribution}' not supported")
        # Validate parameters
        if not all(np.isfinite(params)):
            return None, {"error": "Invalid parameters fitted"}
        # Calculate goodness-of-fit
        gof_stats = calculate_goodness_of_fit(data, distribution, params)
        if "error" in gof_stats:
            return None, gof_stats
        return params, gof_stats
    except Exception as e:
        return None, {"error": f"Fitting failed: {str(e)}"}


def select_best_distribution(
    data: np.ndarray,
    distributions: List[str],
    selection_criteria: str = "aic"
) -> Tuple[str, tuple]:
    """
    Select the best-fitting distribution based on specified criteria.
    Args:
        data: Input data
        distributions: List of distribution names to test
        selection_criteria: Criteria for selection ('aic', 'bic', 'ks', 'rmse')
    Returns:
        Tuple of (best_distribution_name, best_parameters)
    """
    results = {}
    errors = {}
    for dist in distributions:
        params, gof_stats = fit_distribution(data, dist)
        if params is not None and "error" not in gof_stats:
            results[dist] = {
                'params': params,
                'gof': gof_stats
            }
        else:
            errors[dist] = (gof_stats.get(
                "error", "Unknown error"
            ) if gof_stats
                            else "Failed to fit")
        if not results:
            error_msg = (
                "No distributions could be successfully fitted. Errors: "
                + str(errors)
                )
            raise ValueError(error_msg)
        # or
        if not results:
            error_msg = (
                "No distributions could be successfully fitted. Errors: "
                f"{errors}"
            )
            raise ValueError(error_msg)
    # Select best based on criteria
    try:
        if selection_criteria == "aic":
            best_dist = min(
                results.keys(), key=lambda x: results[x]['gof']['aic']
                )
        elif selection_criteria == "bic":
            best_dist = min(
                results.keys(), key=lambda x: results[x]['gof']['bic']
                )
        elif selection_criteria == "ks":
            best_dist = min(
                results.keys(), key=lambda x: results[x]['gof']['ks_stat']
                )
        elif selection_criteria == "rmse":
            best_dist = min(
                results.keys(), key=lambda x: results[x]['gof']['rmse']
                )
        else:
            raise ValueError(
                "Selection criteria must be 'aic', 'bic', 'ks', or 'rmse'"
                )
    except Exception as e:
        raise ValueError(f"Error in distribution selection: {str(e)}")
    return best_dist, results[best_dist]['params']


def calculate_return_period_values(
    distribution: str,
    params: tuple,
    return_periods: List[int]
) -> Dict[str, float]:
    """
    Calculate return period values for a fitted distribution.
    Args:
        distribution: Distribution name
        params: Distribution parameters
        return_periods: List of return periods
    Returns:
        Dictionary with return period values
    """
    # Get distribution object
    if distribution == "gumbel":
        dist_obj = gumbel_r
    elif distribution == "normal":
        dist_obj = norm
    elif distribution == "lognormal":
        dist_obj = lognorm
    elif distribution == "genextreme":
        dist_obj = genextreme
    elif distribution == "gamma":
        dist_obj = gamma
    elif distribution == "weibull":
        dist_obj = weibull_min
    elif distribution == "logpearson3":
        dist_obj = pearson3
    else:
        raise ValueError(f"Distribution '{distribution}' not supported")
    rp_values = {}
    for rp in return_periods:
        prob = 1 - 1 / rp
        try:
            if distribution == "logpearson3":
                log_value = pearson3.ppf(prob, *params)
                value = np.exp(log_value)
            else:
                value = dist_obj.ppf(prob, *params)
            if np.isfinite(value):
                rp_values[f"Q{rp}"] = float(value)
            else:
                rp_values[f"Q{rp}"] = np.nan
        except (ValueError, RuntimeError, np.linalg.LinAlgError):
            rp_values[f"Q{rp}"] = np.nan
    return rp_values


def get_site_statistics_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str
) -> pd.DataFrame:
    """
    Get basic statistics for each site using DuckDB.
    Args:
        conn: DuckDB connection
        table_name: Name of the table
    Returns:
        DataFrame with site statistics
    """
    query = f"""
    SELECT
        site,
        COUNT(annual_peak) as n_years,
        AVG(annual_peak) as mean_flow,
        STDDEV(annual_peak) as std_flow,
        MIN(annual_peak) as min_flow,
        MAX(annual_peak) as max_flow,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY annual_peak) as median_flow
    FROM {table_name}
    WHERE annual_peak IS NOT NULL
    GROUP BY site
    ORDER BY site
    """
    return conn.execute(query).df()


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


# Utility function to create summary statistics using DuckDB
def create_ffa_summary(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create summary statistics from FFA results using DuckDB.
    Args:
        results_df: Results from fit_ffa function
    Returns:
        DataFrame with summary statistics
    """
    conn = duckdb.connect(':memory:')
    try:
        conn.register('ffa_results', results_df)
        # Get summary by distribution type
        summary_query = """
        SELECT
            best_distribution,
            COUNT(*) as n_sites,
            AVG(data_years) as avg_years,
            AVG(outliers_removed) as avg_outliers_removed,
            AVG(mean_flow) as avg_mean_flow,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Q20) as median_Q20
        FROM ffa_results
        WHERE Q20 IS NOT NULL
        GROUP BY best_distribution
        ORDER BY n_sites DESC
        """
        summary_df = conn.execute(summary_query).df()
        return summary_df
    finally:
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
