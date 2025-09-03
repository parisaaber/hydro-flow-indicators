import unittest
import pandas as pd
import duckdb
import requests
from io import BytesIO
import gzip
import os

from raven_api.indicators import (
    mean_annual_flow,
    mean_aug_sep_flow,
    peak_flow_timing,
    days_below_efn,
    peak_flows,
    annual_peaks,
    fit_ffa,
    weekly_flow_exceedance
)


class TestIndicatorsWithRealData(unittest.TestCase):
    """Unit tests for hydrologic indicators using real test data."""

    @classmethod
    def setUpClass(cls):
        """Download, process, and store Parquet test data."""
        url = (
            "https://github.com/parisaaber/hydro-flow-indicators/raw/"
            "refs/heads/main/tests/test_data/Hydrographs.csv.gz"
        )
        r = requests.get(url)
        r.raise_for_status()

        with gzip.open(BytesIO(r.content), mode="rt") as f:
            df_raw = pd.read_csv(f, parse_dates=["date"])

        # Keep numeric columns + date
        numeric_cols = ["date"] + [
            col for col in df_raw.columns
            if col != "date" and pd.api.types.is_numeric_dtype(df_raw[col])
        ]
        df_raw = df_raw[numeric_cols]

        # Convert wide to long format
        df_long = df_raw.melt(
            id_vars=["date"], var_name="site", value_name="value"
        )

        # Keep numeric values only
        df_long = df_long[pd.to_numeric(df_long["value"],
                                        errors="coerce").notnull()]
        df_long["value"] = df_long["value"].astype(float)

        # Add water year
        df_long["month"] = df_long["date"].dt.month
        df_long["year"] = df_long["date"].dt.year
        df_long["water_year"] = df_long["year"]
        df_long.loc[df_long["month"] >= 10, "water_year"] += 1

        # Save temporary Parquet
        cls.parquet_path = "temp_test_data.parquet"
        df_long.to_parquet(cls.parquet_path)

        # DuckDB connection
        cls.con = duckdb.connect()

    @classmethod
    def tearDownClass(cls):
        """Close connection and remove temporary file."""
        cls.con.close()
        try:
            os.remove(cls.parquet_path)
        except PermissionError:
            print(f"Warning: Unable to delete temp file {cls.parquet_path}")

    def test_mean_annual_flow(self):
        df = mean_annual_flow(self.con, self.parquet_path)
        self.assertFalse(df.empty)
        self.assertIn("mean_annual_flow", df.columns)

    def test_mean_aug_sep_flow(self):
        df = mean_aug_sep_flow(self.con, self.parquet_path)
        self.assertFalse(df.empty)
        self.assertIn("mean_aug_sep_flow", df.columns)

    def test_peak_flow_timing(self):
        df = peak_flow_timing(self.con, self.parquet_path)
        self.assertFalse(df.empty)
        self.assertIn("peak_flow_timing", df.columns)

    def test_days_below_efn(self):
        df = days_below_efn(self.con, self.parquet_path, EFN_threshold=0.3)
        self.assertFalse(df.empty)
        self.assertIn("days_below_efn", df.columns)

    def test_peak_flows(self):
        df = peak_flows(self.con, self.parquet_path)
        self.assertFalse(df.empty)
        self.assertIn("mean_annual_peak", df.columns)

    def test_fit_ffa(self):
        # Run fit_ffa on your test data
        peaks_df = annual_peaks(self.con, self.parquet_path)
        ffa_df = fit_ffa(
            peaks_df,
            dist="auto",
            return_periods=[2, 20],
            remove_outliers=False,
            debug=False
        )

        # Check that the returned DataFrame is not empty
        self.assertFalse(ffa_df.empty)

        # Check that essential columns exist
        self.assertIn("site", ffa_df.columns)
        self.assertIn("best_distribution", ffa_df.columns)
        self.assertIn("outliers_removed", ffa_df.columns)
        self.assertIn("Q2", ffa_df.columns)
        self.assertIn("Q20", ffa_df.columns)

    def test_weekly_flow_exceedance(self):
        df = weekly_flow_exceedance(self.con, self.parquet_path)
        self.assertFalse(df.empty)
        self.assertIn("site", df.columns)
        self.assertIn("week", df.columns)
        self.assertIn("p10", df.columns)
        self.assertIn("p50", df.columns)
        self.assertIn("p95", df.columns)


if __name__ == "__main__":
    unittest.main()
