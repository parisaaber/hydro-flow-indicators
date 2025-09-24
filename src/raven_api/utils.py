from typing import List, Dict, Tuple
import numpy as np
import pandas as pd
import scipy.stats as stats
import duckdb
from scipy.stats import (
    gumbel_r, norm, lognorm, genextreme, gamma, weibull_min, pearson3
)


def detect_outliers_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column: str,
    method: str = "iqr",
    threshold: float = 1.5,
    debug: bool = False
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
