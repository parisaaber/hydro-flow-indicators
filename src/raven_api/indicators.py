from __future__ import annotations

import warnings
import uuid
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


# ---------- helpers ----------


def _where_clause(
    sites: Optional[List[str]],
    start_date: Optional[str],
    end_date: Optional[str],
) -> str:
    parts: List[str] = []
    if sites:
        qs = ", ".join("'" + s.replace("'", "''") + "'" for s in sites)
        parts.append(f"site IN ({qs})")
    if start_date and end_date:
        parts.append(f"date BETWEEN '{start_date}' AND '{end_date}'")
    return " AND ".join(parts) if parts else "1=1"


def _filtered_cte(
    parquet_path: str,
    sites: Optional[List[str]],
    start_date: Optional[str],
    end_date: Optional[str],
) -> str:
    """
    Build a common table expression header that selects from the cached main view for this parquet,
    applying request-scoped filters. No global mutable state involved.
    """
    main = CXN.get_or_create_main_view(parquet_path)
    where_clause = _where_clause(sites, start_date, end_date)
    return f"WITH filtered AS (SELECT * FROM {main} WHERE {where_clause})"


# ---------- indicators ----------


def mean_annual_flow(
    parquet_path: str,
    temporal_resolution: str = "overall",
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    if temporal_resolution == "annual":
        q = f"""
        {cte}
        SELECT site, water_year, AVG(value) AS mean_annual_flow
        FROM filtered
        WHERE value IS NOT NULL
        GROUP BY site, water_year
        ORDER BY site, water_year
        """
    else:
        q = f"""
        {cte},
        yearly_means AS (
            SELECT site, water_year, AVG(value) AS maf_per_year
            FROM filtered
            WHERE value IS NOT NULL
            GROUP BY site, water_year
        )
        SELECT site, AVG(maf_per_year) AS mean_annual_flow
        FROM yearly_means
        GROUP BY site
        ORDER BY site
        """
    return CXN.execute(q).fetchdf()


def mean_aug_sep_flow(
    parquet_path: str,
    temporal_resolution: str = "overall",
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    if temporal_resolution == "annual":
        q = f"""
        {cte}
        SELECT site, water_year, AVG(value) AS mean_aug_sep_flow
        FROM filtered
        WHERE EXTRACT(month FROM date) IN (8, 9)
        GROUP BY site, water_year
        ORDER BY site, water_year
        """
    else:
        q = f"""
        {cte},
        aug_sep AS (
            SELECT site, water_year, AVG(value) AS avg_aug_sep_flow
            FROM filtered
            WHERE EXTRACT(month FROM date) IN (8, 9)
            GROUP BY site, water_year
        )
        SELECT site, AVG(avg_aug_sep_flow) AS mean_aug_sep_flow
        FROM aug_sep
        GROUP BY site
        ORDER BY site
        """
    return CXN.execute(q).fetchdf()


def peak_flow_timing(
    parquet_path: str,
    temporal_resolution: str = "overall",
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    if temporal_resolution == "annual":
        q = f"""
        {cte},
        ranked_flows AS (
            SELECT site, water_year, value,
                   EXTRACT(doy FROM date) AS doy,
                   ROW_NUMBER() OVER (PARTITION BY site, water_year ORDER BY value DESC) AS rn
            FROM filtered
        )
        SELECT site, water_year, doy AS peak_flow_timing
        FROM ranked_flows
        WHERE rn = 1
        ORDER BY site, water_year
        """
    else:
        q = f"""
        {cte},
        ranked_flows AS (
            SELECT site, water_year, value,
                   EXTRACT(doy FROM date) AS doy,
                   ROW_NUMBER() OVER (PARTITION BY site, water_year ORDER BY value DESC) AS rn
            FROM filtered
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
    return CXN.execute(q).fetchdf()


def days_below_efn(
    parquet_path: str,
    EFN_threshold: float,
    temporal_resolution: str = "overall",
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    base = f"""
    {cte},
    mean_annual AS (
        SELECT site, water_year, AVG(value) AS maf_year
        FROM filtered
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
        FROM filtered p
        JOIN mean_annual_site m ON p.site = m.site
    )
    """
    if temporal_resolution == "annual":
        q = f"""
        {base},
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
        q = f"""
        {base},
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
    return CXN.execute(q).fetchdf()


def annual_peaks(
    parquet_path: str,
    temporal_resolution: str = "overall",
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    if temporal_resolution == "annual":
        q = f"""
        {cte}
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM filtered
        WHERE value IS NOT NULL
        GROUP BY site, water_year
        ORDER BY site, water_year
        """
    else:
        q = f"""
        {cte},
        annual_peaks AS (
            SELECT site, water_year, MAX(value) AS annual_peak
            FROM filtered
            WHERE value IS NOT NULL
            GROUP BY site, water_year
        )
        SELECT site, AVG(annual_peak) AS annual_peak
        FROM annual_peaks
        GROUP BY site
        ORDER BY site
        """
    return CXN.execute(q).fetchdf()


def fit_ffa(
    parquet_path: str,
    dist: str = "auto",
    return_periods: List[int] = [2, 20],
    remove_outliers: bool = False,
    outlier_method: str = "iqr",
    outlier_threshold: float = 1.5,
    available_distributions: List[str] = (
        ["gumbel", "genextreme", "normal", "lognormal", "gamma", "logpearson3"]
    ),
    selection_criteria: str = "aic",
    min_years: int = 5,
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    debug: bool = False,
) -> pd.DataFrame:
    """
    FFA using request-scoped CTE and a unique TEMP view (no collisions).
    """
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    temp_view = f"temp_annual_peaks_{uuid.uuid4().hex[:8]}"

    CXN.con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {temp_view} AS
        {cte}
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM filtered
        WHERE value IS NOT NULL
        GROUP BY site, water_year
        """
    )

    try:
        site_counts_df = CXN.execute(
            f"""
            SELECT site, COUNT(*) AS n_years
            FROM {temp_view}
            GROUP BY site
            HAVING COUNT(*) >= {min_years}
            """
        ).fetchdf()

        if site_counts_df.empty:
            warnings.warn(f"No sites have at least {min_years} years of data.")
            return pd.DataFrame()

        valid_sites = site_counts_df["site"].tolist()
        site_stats_df = get_site_statistics_duckdb(CXN.con, temp_view).set_index("site")

        results = []
        for site in valid_sites:
            site_q = f"""
                SELECT *
                FROM {temp_view}
                WHERE site = '{site.replace("'", "''")}'
                ORDER BY water_year
            """
            site_data = CXN.execute(site_q).fetchdf()
            values = site_data["annual_peak"].values
            outliers_removed = 0

            if remove_outliers:
                CXN.con.register("temp_site_data", site_data)
                outlier_query = detect_outliers_duckdb(
                    "temp_site_data",
                    "annual_peak",
                    outlier_method,
                    outlier_threshold,
                )
                site_data = CXN.execute(outlier_query).fetchdf()
                outliers_removed = int(site_data["is_outlier"].sum())
                values = site_data.loc[~site_data["is_outlier"], "annual_peak"].values
                if len(values) < min_years:
                    warnings.warn(f"Site {site}: Not enough data after outlier removal")
                    continue

            try:
                if dist == "auto":
                    best_dist, best_params = select_best_distribution(
                        values, available_distributions, selection_criteria
                    )
                else:
                    best_dist = dist
                    best_params, _ = fit_distribution(values, dist)
                    if best_params is None:
                        warnings.warn(f"Site {site}: Could not fit {dist} distribution")
                        continue

                rp_vals = calculate_return_period_values(
                    best_dist, best_params, return_periods
                )
                site_stats = site_stats_df.loc[site].to_dict()

                row = {
                    "site": site,
                    "best_distribution": best_dist,
                    "outliers_removed": outliers_removed,
                    "data_years": len(values),
                }
                row.update(site_stats)
                row.update(rp_vals)
                results.append(row)

            except Exception as e:
                warnings.warn(f"Site {site}: Error in FFA - {e}")
                if debug:
                    print(f"Error site {site}: {e}")
                continue

        if not results:
            warnings.warn("No sites could be successfully analyzed.")
            return pd.DataFrame()

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

    finally:
        try:
            CXN.execute(f"DROP VIEW IF EXISTS {temp_view}")
        except Exception:
            pass


def peak_flows(
    parquet_path: str,
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    q = f"""
    {cte},
    annual_peaks AS (
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM filtered
        WHERE value IS NOT NULL
        GROUP BY site, water_year
    )
    SELECT site, AVG(annual_peak) AS mean_annual_peak
    FROM annual_peaks
    GROUP BY site
    ORDER BY site
    """
    return CXN.execute(q).fetchdf()


def weekly_flow_exceedance(
    parquet_path: str,
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    q = f"""
    {cte}
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
    FROM filtered
    WHERE value IS NOT NULL
    GROUP BY site, CAST(strftime('%W', date) AS INTEGER)
    ORDER BY site, week
    """
    return CXN.execute(q).fetchdf()


def calculate_all_indicators(
    parquet_path: str,
    EFN_threshold: float = 0.2,
    return_periods: List[int] = [2, 20],
    remove_outliers: bool = False,
    outlier_method: str = "iqr",
    outlier_threshold: float = 1.5,
    dist: str = "auto",
    available_distributions: List[str] = (
        ["gumbel", "genextreme", "normal", "lognormal", "gamma", "logpearson3"]
    ),
    selection_criteria: str = "aic",
    min_years: int = 5,
    debug: bool = False,
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute the full indicator set from a request-scoped filtered subset.
    """
    try:
        maf = mean_annual_flow(
            parquet_path,
            "overall",
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        ).set_index("site")
        aug_sep = mean_aug_sep_flow(
            parquet_path,
            "overall",
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        ).set_index("site")
        timing = peak_flow_timing(
            parquet_path,
            "overall",
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        ).set_index("site")
        efn_days = days_below_efn(
            parquet_path,
            EFN_threshold,
            "overall",
            sites=sites,
            start_date=start_date,
            end_date=end_date,
        ).set_index("site")
        peaks = peak_flows(
            parquet_path, sites=sites, start_date=start_date, end_date=end_date
        ).set_index("site")

        indicators = pd.concat([maf, aug_sep, timing, efn_days, peaks], axis=1)

        ffa = fit_ffa(
            parquet_path,
            dist=dist,
            return_periods=return_periods,
            remove_outliers=remove_outliers,
            outlier_method=outlier_method,
            outlier_threshold=outlier_threshold,
            selection_criteria=selection_criteria,
            min_years=min_years,
            sites=sites,
            start_date=start_date,
            end_date=end_date,
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

    # sanitize NaNs/Infs
    final_df = final_df.replace([np.inf, -np.inf, np.nan], None)

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

    ordered = base_cols + indicator_cols + rp_cols + ffa_meta_cols
    final_cols = [c for c in ordered if c in final_df.columns]
    remaining = [c for c in final_df.columns if c not in final_cols]
    final_cols.extend(remaining)

    final_df = final_df[final_cols]
    if debug:
        print(f"\nFinal results: {len(final_df)} rows, {len(final_df.columns)} columns")
        print(f"Sites: {final_df['site'].nunique()}")
    return final_df


def aggregate_flows(
    parquet_path: str,
    temporal_resolution: str = "daily",
    *,
    sites: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
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
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) IN (12, 1, 2) THEN 'Winter' "
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) BETWEEN 3 AND 5 THEN 'Spring' "
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) BETWEEN 6 AND 8 THEN 'Summer' "
            "WHEN CAST(substr(strftime('%m', date), 1, 2) AS INTEGER) BETWEEN 9 AND 11 THEN 'Fall' "
            "END"
        )
    else:
        raise ValueError(
            "Invalid temporal_resolution: choose 'daily'|'weekly'|'monthly'|'seasonal'"
        )

    cte = _filtered_cte(parquet_path, sites, start_date, end_date)
    q = f"""
        {cte}
        SELECT
            site,
            {group_expr} AS period,
            AVG(value) AS mean_flow
        FROM filtered
        WHERE value IS NOT NULL
        GROUP BY site, period
        ORDER BY site, period
    """
    df = CXN.execute(q).fetchdf()
    return df.rename(columns={"period": "time_period"})
