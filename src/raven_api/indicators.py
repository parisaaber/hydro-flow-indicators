import warnings
from typing import List, Optional

import numpy as np
import pandas as pd

from .connection import Connection
from .utils import (
    calculate_return_period_values,
    detect_outliers_duckdb,
    fit_distribution,
    select_best_distribution,
    get_site_statistics_duckdb,
)

CXN = Connection()


def mean_annual_flow(temporal_resolution: str = "overall") -> pd.DataFrame:
    """
    Calculate mean annual flow using the globally configured CXN.
    Assumes CXN has already been configured via /configure endpoint.
    """
    filtered_view = CXN.create_filtered_data_view("temp_annual_view")
    if temporal_resolution == "annual":
        query = f"""
            SELECT site, water_year, AVG(value) AS mean_annual_flow
            FROM {filtered_view}
            WHERE value IS NOT NULL
            GROUP BY site, water_year
            ORDER BY site, water_year
        """
    else:
        query = f"""
            WITH yearly_means AS (
                SELECT site, water_year, AVG(value) AS maf_per_year
                FROM {filtered_view}
                WHERE value IS NOT NULL
                GROUP BY site, water_year
            )
            SELECT site, AVG(maf_per_year) AS mean_annual_flow
            FROM yearly_means
            GROUP BY site
            ORDER BY site
        """
    return CXN.execute(query).fetchdf()


def mean_aug_sep_flow(temporal_resolution: str = "overall") -> pd.DataFrame:
    """
    Calculate mean August–September flow using globally configured CXN.
    Assumes CXN has already been configured via /configure endpoint.
    """
    if temporal_resolution == "annual":
        filtered_view = CXN.create_filtered_data_view("temp_aug_sep_view")
        query = f"""
            SELECT site, water_year, AVG(value) AS mean_aug_sep_flow
            FROM {filtered_view}
            WHERE EXTRACT(month FROM date) IN (8, 9)
            GROUP BY site, water_year
            ORDER BY site, water_year
        """
    else:
        filtered_view = CXN.create_filtered_data_view("temp_aug_sep_view")
        query = f"""
            WITH aug_sep AS (
                SELECT site, water_year, AVG(value) AS avg_aug_sep_flow
                FROM {filtered_view}
                WHERE EXTRACT(month FROM date) IN (8, 9)
                GROUP BY site, water_year
            )
            SELECT site, AVG(avg_aug_sep_flow) AS mean_aug_sep_flow
            FROM aug_sep
            GROUP BY site
            ORDER BY site
        """
    return CXN.execute(query).fetchdf()


def peak_flow_timing(temporal_resolution: str = "overall") -> pd.DataFrame:
    """
    Estimate the average day of year of annual peak flow using global CXN.
    """
    filtered_view = CXN.create_filtered_data_view("temp_peak_flow_view")

    if temporal_resolution == "annual":
        query = f"""
            WITH ranked_flows AS (
                SELECT site, water_year, value,
                EXTRACT(doy FROM date) AS doy,
                ROW_NUMBER() OVER (
                PARTITION BY site, water_year
                ORDER BY value DESC
                ) AS rn
                FROM {filtered_view}
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
                ROW_NUMBER() OVER (
                PARTITION BY site, water_year
                ORDER BY value DESC
                ) AS rn
                FROM {filtered_view}
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
    return CXN.execute(query).fetchdf()


def days_below_efn(
    EFN_threshold: float, temporal_resolution: str = "overall"
) -> pd.DataFrame:
    """
    Calculate days below EFN threshold using globally configured CXN.
    """
    filtered_view = CXN.create_filtered_data_view("temp_efn_view")

    query = f"""
        WITH mean_annual AS (
            SELECT site, water_year, AVG(value) AS maf_year
            FROM {filtered_view}
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
            FROM {filtered_view} p
            JOIN mean_annual_site m ON p.site = m.site
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
    return CXN.execute(query).fetchdf()


def annual_peaks(temporal_resolution: str = "overall") -> pd.DataFrame:
    """
    Extract annual peak flow for each site and water year
    using globally configured CXN.

    Args:
        temporal_resolution: 'overall' for average peak per site,
                            'annual' for per water year values.

    Returns:
        DataFrame with 'site', 'water_year' (if annual),
        and 'annual_peak' columns.
    """
    filtered_view = CXN.create_filtered_data_view("temp_annual_peaks_view")

    if temporal_resolution == "annual":
        query = f"""
            SELECT site, water_year, MAX(value) AS annual_peak
            FROM {filtered_view}
            WHERE value IS NOT NULL
            GROUP BY site, water_year
            ORDER BY site, water_year
        """
    else:
        query = f"""
            WITH annual_peaks AS (
                SELECT site, water_year, MAX(value) AS annual_peak
                FROM {filtered_view}
                WHERE value IS NOT NULL
                GROUP BY site, water_year
            )
            SELECT site, AVG(annual_peak) AS annual_peak
            FROM annual_peaks
            GROUP BY site
            ORDER BY site
        """

    return CXN.execute(query).fetchdf()


def fit_ffa(
    dist: str = "auto",
    return_periods: List[int] = [2, 20],
    remove_outliers: bool = False,
    outlier_method: str = "iqr",
    outlier_threshold: float = 1.5,
    available_distributions: List[str] = [
        "gumbel",
        "genextreme",
        "normal",
        "lognormal",
        "gamma",
        "logpearson3",
    ],
    selection_criteria: str = "aic",
    min_years: int = 5,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Flood Frequency Analysis using CXN/SQL-heavy approach.
    Matches original version output and logic.
    """
    # 1️⃣ Create filtered data view for peaks
    filtered_view = CXN.create_filtered_data_view("temp_peak_flows_view")

    # 2️⃣ Create a SQL view for annual peaks per site/year
    CXN.con.execute(
        f"""
        CREATE OR REPLACE VIEW temp_annual_peaks AS
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM {filtered_view}
        WHERE value IS NOT NULL
        GROUP BY site, water_year
    """
    )

    # 3️⃣ Get site counts to filter sites with enough data
    site_counts_df = CXN.execute(
        f"""
        SELECT site, COUNT(*) AS n_years
        FROM temp_annual_peaks
        GROUP BY site
        HAVING COUNT(*) >= {min_years}
    """
    ).fetchdf()

    if site_counts_df.empty:
        warnings.warn(f"No sites have at least {min_years} years of data.")
        return pd.DataFrame()

    valid_sites = site_counts_df["site"].tolist()

    # 4️⃣ Prepare site statistics via SQL
    site_stats_df = get_site_statistics_duckdb(
        CXN.con, "temp_annual_peaks"
    ).set_index("site")

    results = []

    for site in valid_sites:
        # 5️⃣ Extract site data via SQL
        site_data = CXN.execute(
            f"""
            SELECT *
            FROM temp_annual_peaks
            WHERE site = '{site}'
            ORDER BY water_year
        """
        ).fetchdf()

        values = site_data["annual_peak"].values
        outliers_removed = 0

        # 6️⃣ Remove outliers using SQL view if requested
        if remove_outliers:
            CXN.con.register("temp_site_data", site_data)
            outlier_query = detect_outliers_duckdb(
                CXN.con,
                "temp_site_data",
                "annual_peak",
                outlier_method,
                outlier_threshold,
                debug=debug,
            )
            site_data = CXN.execute(outlier_query).fetchdf()
            outliers_removed = int(site_data["is_outlier"].sum())
            values = site_data.loc[
                ~site_data["is_outlier"], "annual_peak"
            ].values

            if len(values) < min_years:
                warnings.warn(
                    f"Site {site}: Not enough data after outlier removal"
                )
                continue

        # 7️⃣ Distribution fitting in Python
        try:
            if dist == "auto":
                best_dist, best_params = select_best_distribution(
                    values, available_distributions, selection_criteria
                )
            else:
                best_dist = dist
                best_params, _ = fit_distribution(values, dist)
                if best_params is None:
                    warnings.warn(
                        f"Site {site}: Could not fit {dist} distribution"
                    )
                    continue

            # 8️⃣ Return period values in Python
            rp_values = calculate_return_period_values(
                best_dist, best_params, return_periods
            )

            # 9️⃣ Pull site statistics from DuckDB
            site_stats = site_stats_df.loc[site].to_dict()

            # 1️⃣0️⃣ Compile final result for this site
            site_result = {
                "site": site,
                "best_distribution": best_dist,
                "outliers_removed": outliers_removed,
                "data_years": len(values),
            }
            site_result.update(site_stats)
            site_result.update(rp_values)
            results.append(site_result)

        except Exception as e:
            warnings.warn(f"Site {site}: Error in FFA - {e}")
            if debug:
                print(f"Error site {site}: {e}")
            continue

    if not results:
        warnings.warn("No sites could be successfully analyzed.")
        return pd.DataFrame()

    # 1️⃣1️⃣ Convert to DataFrame and add SQL-based ranking
    results_df = pd.DataFrame(results)
    CXN.con.register("results_data", results_df)
    final_results = CXN.execute(
        """
        SELECT *,
            RANK() OVER (ORDER BY mean_flow DESC) AS flow_rank
        FROM results_data
        ORDER BY flow_rank
    """
    ).fetchdf()

    return final_results


def peak_flows() -> pd.DataFrame:
    """
    Calculate mean annual peak flow for each site
    using the globally configured CXN.

    Returns:
        DataFrame with 'site' and 'mean_annual_peak' columns.
    """
    filtered_view = CXN.create_filtered_data_view("temp_peak_flows_view")
    query = f"""
        WITH annual_peaks AS (
            SELECT site, water_year, MAX(value) AS annual_peak
            FROM {filtered_view}
            WHERE value IS NOT NULL
            GROUP BY site, water_year
        )
        SELECT site, AVG(annual_peak) AS mean_annual_peak
        FROM annual_peaks
        GROUP BY site
        ORDER BY site
    """
    return CXN.execute(query).fetchdf()


def weekly_flow_exceedance() -> pd.DataFrame:
    """
    Compute weekly flow exceedance for the specified sites
    and period using the globally configured CXN.

    Returns:
        DataFrame with weekly flow exceedance percentiles for each site.
    """
    filtered_view = CXN.create_filtered_data_view(
        "temp_weekly_exceedance_view"
    )
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
    FROM {filtered_view}
    WHERE value IS NOT NULL
    GROUP BY
        site,
        CAST(strftime('%W', date) AS INTEGER)
    ORDER BY site, week
    """
    return CXN.execute(query).fetchdf()


def calculate_all_indicators(
    EFN_threshold: float = 0.2,
    return_periods: List[int] = [2, 20],
    remove_outliers: bool = False,
    outlier_method: str = "iqr",
    outlier_threshold: float = 1.5,
    dist: str = "auto",
    available_distributions: List[str] = [
        "gumbel",
        "genextreme",
        "normal",
        "lognormal",
        "gamma",
        "logpearson3",
    ],
    selection_criteria: str = "aic",
    min_years: int = 5,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Calculate a comprehensive set of hydrologic indicators
    for one or more subperiods
    using the globally configured CXN.

    Indicators include:
    - Mean annual flow
    - August-September mean flow
    - Peak flow timing (DOY)
    - Number of days below EFN threshold
    - Mean annual peak flow
    - Flood frequency analysis with return period quantiles

    Args:
        EFN_threshold: Proportion of MAF used for
        low flow threshold (e.g., 0.2).
        return_periods: List of return periods for flood frequency analysis.
        remove_outliers: Whether to remove outliers in FFA.
        outlier_method: Method for outlier detection
        ('iqr', 'zscore', 'modified_zscore').
        outlier_threshold: Threshold for outlier detection.
        dist: Distribution for FFA ('auto' or specific distribution name).
        available_distributions: List of distributions to test if dist='auto'.
        selection_criteria: Criteria for distribution selection
        ('aic', 'bic', 'ks').
        min_years: Minimum years required for analysis.
        debug: Enable debug output.

    Returns:
        DataFrame containing calculated indicators for each site and subperiod.
    """
    # Get the current configuration to understand the data structure
    config = CXN.get_config()
    if not config.get("parquet_path"):
        raise RuntimeError(
            "CXN connection not configured. Please run CXN.configure() first."
        )

    try:
        if debug:
            print("Calculating mean annual flow...")
        maf = mean_annual_flow(temporal_resolution="overall").set_index("site")

        if debug:
            print("Calculating mean August-September flow...")
        aug_sep = mean_aug_sep_flow(temporal_resolution="overall").set_index(
            "site"
        )

        if debug:
            print("Calculating peak flow timing...")
        timing = peak_flow_timing(temporal_resolution="overall").set_index(
            "site"
        )

        if debug:
            print("Calculating days below EFN...")
        efn_days = days_below_efn(
            EFN_threshold, temporal_resolution="overall"
        ).set_index("site")

        if debug:
            print("Calculating mean annual peaks...")
        peaks = peak_flows().set_index("site")

        indicators = pd.concat([maf, aug_sep, timing, efn_days, peaks], axis=1)

        if debug:
            print("Performing flood frequency analysis...")
        ffa = fit_ffa(
            dist=dist,
            return_periods=return_periods,
            remove_outliers=remove_outliers,
            outlier_method=outlier_method,
            outlier_threshold=outlier_threshold,
            available_distributions=available_distributions,
            selection_criteria=selection_criteria,
            min_years=min_years,
            debug=debug,
        )

        if not ffa.empty:
            all_indicators = indicators.join(ffa.set_index("site"), how="left")
        else:
            all_indicators = indicators
            for rp in return_periods:
                all_indicators[f"Q{rp}"] = None
            all_indicators["best_distribution"] = None
            all_indicators["outliers_removed"] = None
            all_indicators["data_years"] = None
            all_indicators["flow_rank"] = None
            all_indicators["std_flow"] = None
            all_indicators["min_flow"] = None
            all_indicators["max_flow"] = None

        all_indicators["efn_threshold"] = EFN_threshold
        final_df = all_indicators.reset_index()

    except Exception as e:
        warnings.warn(f"Error calculating indicators: {e}")
        if debug:
            print(f"Error: {e}")
        return pd.DataFrame()

    # Clean up infinities and NaNs
    final_df = final_df.replace([np.inf, -np.inf, np.nan], None)

    # Reorder columns
    base_cols = ["site", "efn_threshold"]
    indicator_cols = [
        "mean_annual_flow",
        "mean_aug_sep_flow",
        "peak_flow_timing",
        "days_below_efn",
        "mean_annual_peak",
    ]
    rp_cols = [f"Q{rp}" for rp in return_periods]
    ffa_meta_cols = [
        "best_distribution",
        "outliers_removed",
        "data_years",
        "std_flow",
        "min_flow",
        "max_flow",
        "flow_rank",
    ]

    ordered_cols = base_cols + indicator_cols + rp_cols + ffa_meta_cols
    final_cols = [c for c in ordered_cols if c in final_df.columns]
    remaining_cols = [c for c in final_df.columns if c not in final_cols]
    final_cols.extend(remaining_cols)

    final_df = final_df[final_cols]

    if debug:
        print(
            f"\nFinal results: {len(final_df)} rows,"
            f" {len(final_df.columns)} columns"
        )
        print(f"Sites: {final_df['site'].nunique()}")

    return final_df


def aggregate_flows(temporal_resolution: str = "daily") -> pd.DataFrame:
    """
    Aggregate flows over different temporal resolutions
    using globally configured CXN.
    """
    # Use main_data directly instead of creating a filtered view
    view_name = "filtered_data"

    if temporal_resolution == "daily":
        group_expr = "date"
    elif temporal_resolution == "weekly":
        group_expr = "strftime('%Y-%W', date)"
    elif temporal_resolution == "monthly":
        group_expr = "strftime('%Y-%m', date)"
    elif temporal_resolution == "seasonal":
        group_expr = (
            "strftime('%Y', date) || '-' || "
            "CASE "
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) "
            "IN (12, 1, 2) THEN 'Winter' "
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) "
            "BETWEEN 3 AND 5 THEN 'Spring' "
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) "
            "BETWEEN 6 AND 8 THEN 'Summer' "
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) "
            "BETWEEN 9 AND 11 THEN 'Fall' "
            "END"
        )
    else:
        raise ValueError(
            "Invalid temporal_resolution: choose from 'daily',"
            "'weekly', 'monthly', 'seasonal'"
        )

    query = f"""
        SELECT
            site,
            {group_expr} AS period,
            AVG(value) AS mean_flow
        FROM {view_name}
        WHERE value IS NOT NULL
        GROUP BY site, period
        ORDER BY site, period
    """

    df = CXN.execute(query).fetchdf()
    return df.rename(columns={"period": "time_period"})


def get_connection_status() -> dict:
    """Get current connection configuration and status"""
    return CXN.get_config()


def reset_connection():
    """Reset the global connection configuration"""
    CXN.reset_config()


def configure_connection(
    parquet_path: Optional[str] = None,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """Configure the global connection"""
    CXN.configure(
        parquet_path=parquet_path,
        sites=sites,
        start_date=start_date,
        end_date=end_date,
    )
