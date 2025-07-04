import unittest
import pandas as pd
import duckdb
import tempfile
import os
import gzip
import requests
from io import BytesIO

from raven_api.indicators import (
    mean_annual_flow,
    mean_aug_sep_flow,
    peak_flow_timing,
    days_below_efn,
    peak_flows,
    annual_peaks,
    fit_ffa
)

class TestIndicatorsWithRealData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Download and read test data
        url = "https://github.com/parisaaber/hydro-flow-indicators/raw/refs/heads/main/tests/test_data/Hydrographs.csv.gz"
        r = requests.get(url)
        r.raise_for_status()

        with gzip.open(BytesIO(r.content), mode="rt") as f:
            df_raw = pd.read_csv(f, parse_dates=["date"])

        # Keep only numeric columns + date
        numeric_cols = ['date'] + [
            col for col in df_raw.columns if col != "date" and pd.api.types.is_numeric_dtype(df_raw[col])
        ]
        df_raw = df_raw[numeric_cols]

        # Convert wide to long format
        df_long = df_raw.melt(id_vars=["date"], var_name="site", value_name="value")

        # Remove any non-numeric values just in case
        df_long = df_long[pd.to_numeric(df_long["value"], errors="coerce").notnull()]
        df_long["value"] = df_long["value"].astype(float)

        # Add water year
        df_long["month"] = df_long["date"].dt.month
        df_long["year"] = df_long["date"].dt.year
        df_long["water_year"] = df_long["year"]
        df_long.loc[df_long["month"] >= 10, "water_year"] += 1

        # Write to temporary Parquet file
        cls.temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df_long.to_parquet(cls.temp_file.name)
        cls.parquet_path = cls.temp_file.name

        # DuckDB connection
        cls.con = duckdb.connect()

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        cls.temp_file.close()
        try:
            os.remove(cls.temp_file.name)
        except PermissionError:
            print(f"Warning: Unable to delete temp file {cls.temp_file.name} (maybe still in use)")

    def test_mean_annual_flow(self):
        result = mean_annual_flow(self.con, self.parquet_path)
        self.assertFalse(result.empty)
        self.assertIn("mean_annual_flow", result.columns)

    def test_mean_aug_sep_flow(self):
        result = mean_aug_sep_flow(self.con, self.parquet_path)
        self.assertFalse(result.empty)
        self.assertIn("mean_aug_sep_flow", result.columns)

    def test_peak_flow_timing(self):
        result = peak_flow_timing(self.con, self.parquet_path)
        self.assertFalse(result.empty)
        self.assertIn("peak_flow_timing", result.columns)

    def test_days_below_efn(self):
        result = days_below_efn(self.con, self.parquet_path, EFN_threshold=0.3)
        self.assertFalse(result.empty)
        self.assertIn("days_below_efn", result.columns)

    def test_peak_flows(self):
        result = peak_flows(self.con, self.parquet_path)
        self.assertFalse(result.empty)
        self.assertIn("mean_annual_peak", result.columns)

    def test_annual_peaks(self):
        result = annual_peaks(self.con, self.parquet_path)
        self.assertFalse(result.empty)
        self.assertIn("annual_peak", result.columns)

    def test_fit_ffa(self):
        peaks = annual_peaks(self.con, self.parquet_path)
        result = fit_ffa(peaks, return_periods=[2, 20])
        self.assertFalse(result.empty)
        self.assertIn("Q2", result.columns)
        self.assertIn("Q20", result.columns)

if __name__ == '__main__':
    unittest.main()
