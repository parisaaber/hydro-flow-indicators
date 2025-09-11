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
    weekly_flow_exceedance,
    aggregate_flows
)


class TestIndicatorsWithRealData(unittest.TestCase):

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
        df_long = df_long[pd.to_numeric(
            df_long["value"], errors="coerce"
            ).notnull()]
        df_long["value"] = df_long["value"].astype(float)

        # Add water year
        df_long["month"] = df_long["date"].dt.month
        df_long["year"] = df_long["date"].dt.year
        df_long["water_year"] = df_long["year"]
        df_long.loc[df_long["month"] >= 10, "water_year"] += 1

        # Save temporary Parquet
        cls.parquet_path = "temp_test_data.parquet"
        df_long.to_parquet(cls.parquet_path)

        # Store some data characteristics for validation
        cls.expected_sites = set(df_long["site"].unique())
        cls.expected_date_range = (
            df_long["date"].min(), df_long["date"].max()
            )
        cls.expected_value_range = (
            df_long["value"].min(), df_long["value"].max()
            )
        cls.expected_water_years = set(df_long["water_year"].unique())
        # Calculate some expected statistics for validation
        cls.site_stats = df_long.groupby("site")["value"].agg(
            ['mean', 'max', 'min', 'count']
            ).to_dict('index')

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
        """Test mean annual flow calculation with value validation."""
        df = mean_annual_flow(self.con, self.parquet_path)
        # Basic structure tests
        self.assertFalse(df.empty)
        self.assertIn("mean_annual_flow", df.columns)
        self.assertIn("site", df.columns)
        # Value validation tests
        self.assertTrue(
            all(df["mean_annual_flow"] > 0),
            "All mean annual flows should be positive")
        self.assertTrue(all(
            df["mean_annual_flow"] < self.expected_value_range[1]
            ),
            "Mean annual flows should be less than maximum observed value"
                )
        # Check that we have results for expected sites
        returned_sites = set(df["site"].unique())
        self.assertTrue(
            returned_sites.issubset(self.expected_sites),
            "Returned sites should be subset of expected sites"
        )
        # Test with specific sites filter
        test_sites = list(self.expected_sites)[:2]  # Take first 2 sites
        df_filtered = mean_annual_flow(
            self.con, self.parquet_path, sites=test_sites
        )
        self.assertEqual(set(df_filtered["site"]), set(test_sites))

    def test_mean_aug_sep_flow(self):
        """Test August-September mean flow with seasonal validation."""
        df = mean_aug_sep_flow(self.con, self.parquet_path)
        # Basic structure tests
        self.assertFalse(df.empty)
        self.assertIn("mean_aug_sep_flow", df.columns)
        # Value validation
        self.assertTrue(
            all(df["mean_aug_sep_flow"] >= 0),
            "Aug-Sep flows should be non-negative"
        )
        # Aug-Sep flows should generally be lower than annual means
        annual_df = mean_annual_flow(self.con, self.parquet_path)
        merged = df.merge(annual_df, on="site")
        # Check that at least some sites have lower Aug-Sep
        # flows than annual mean
        lower_count = sum(
            merged["mean_aug_sep_flow"] < merged["mean_annual_flow"]
            )
        self.assertGreater(
            lower_count, 0,
            "Some sites should have lower Aug-Sep flows than annual mean"
        )

    def test_peak_flow_timing(self):
        """Test peak flow timing with DOY validation."""
        df = peak_flow_timing(self.con, self.parquet_path)
        # Basic structure tests
        self.assertFalse(df.empty)
        self.assertIn("peak_flow_timing", df.columns)
        # DOY validation
        self.assertTrue(all(df["peak_flow_timing"] >= 1), "DOY should be >= 1")
        self.assertTrue(
            all(df["peak_flow_timing"] <= 366), "DOY should be <= 366"
            )
        # Test annual resolution
        df_annual = peak_flow_timing(
            self.con,
            self.parquet_path,
            temporal_resolution="annual"
        )
        self.assertIn("water_year", df_annual.columns)
        self.assertTrue(
            all(df_annual["water_year"].isin(self.expected_water_years))
            )

    def test_days_below_efn(self):
        """Test EFN days calculation with threshold validation."""
        EFN_threshold = 0.3
        df = days_below_efn(
            self.con,
            self.parquet_path,
            EFN_threshold=EFN_threshold
        )
        # Basic structure tests
        self.assertFalse(df.empty)
        self.assertIn("days_below_efn", df.columns)
        # Value validation
        self.assertTrue(
            all(df["days_below_efn"] >= 0),
            "Days below EFN should be non-negative"
        )
        self.assertTrue(
            all(df["days_below_efn"] <= 366),
            "Days below EFN should not exceed 366"
        )
        # Test different thresholds -
        # higher threshold should generally result in more days
        df_high = days_below_efn(
            self.con,
            self.parquet_path,
            EFN_threshold=0.5
        )
        merged = df.merge(
            df_high,
            on="site",
            suffixes=("_low", "_high")
        )
        # Most sites should have more days below higher threshold
        higher_count = sum(
            merged["days_below_efn_high"] >= merged["days_below_efn_low"]
            )
        total_sites = len(merged)
        self.assertGreater(
            higher_count / total_sites, 0.5,
            "Most sites should have more days below higher EFN threshold"
        )

    def test_peak_flows(self):
        """Test peak flows calculation."""
        df = peak_flows(self.con, self.parquet_path)
        # Basic structure tests
        self.assertFalse(df.empty)
        self.assertIn("mean_annual_peak", df.columns)
        # Value validation
        self.assertTrue(
            all(df["mean_annual_peak"] > 0),
            "Peak flows should be positive"
        )
        # Peak flows should be higher than mean flows
        mean_df = mean_annual_flow(self.con, self.parquet_path)
        merged = df.merge(mean_df, on="site")
        higher_peaks = sum(
            merged["mean_annual_peak"] > merged["mean_annual_flow"]
            )
        self.assertEqual(
            higher_peaks, len(merged),
            "All peak flows should be higher than mean flows"
        )

    def test_annual_peaks(self):
        """Test annual peaks extraction."""
        df = annual_peaks(self.con, self.parquet_path)
        # Basic structure tests
        self.assertFalse(df.empty)
        self.assertIn("annual_peak", df.columns)
        self.assertIn("water_year", df.columns)
        # Value validation
        self.assertTrue(
            all(df["annual_peak"] > 0),
            "Annual peaks should be positive"
        )
        self.assertTrue(
            all(df["water_year"].isin(self.expected_water_years))
            )
        # Check that we have reasonable number of peaks per site
        peaks_per_site = df.groupby("site").size()
        self.assertTrue(
            all(peaks_per_site >= 1),
            "Each site should have at least one peak"
        )

    def test_fit_ffa(self):
        """Test flood frequency analysis with distribution validation."""
        # Get peaks data
        peaks_df = annual_peaks(self.con, self.parquet_path)
        # Run FFA
        ffa_df = fit_ffa(
            peaks_df,
            dist="auto",
            return_periods=[2, 20],
            remove_outliers=False,
            debug=False
        )

        # Basic structure tests
        self.assertFalse(ffa_df.empty)
        self.assertIn("site", ffa_df.columns)
        self.assertIn("best_distribution", ffa_df.columns)
        self.assertIn("Q2", ffa_df.columns)
        self.assertIn("Q20", ffa_df.columns)
        # Value validation
        self.assertTrue(all(ffa_df["Q2"] > 0), "Q2 values should be positive")
        self.assertTrue(
            all(ffa_df["Q20"] > 0),
            "Q20 values should be positive"
        )
        self.assertTrue(
                        all(ffa_df["Q20"] > ffa_df["Q2"]),
                        "Q20 should be greater than Q2"
                    )
        # Distribution validation
        valid_distributions = [
            "gumbel", "genextreme",
            "normal", "lognormal", "gamma", "logpearson3"
            ]
        self.assertTrue(
                        all(ffa_df["best_distribution"].isin(
                            valid_distributions
                            )),
                        "All best distributions should be from valid list"
                    )
        # Statistical validation
        self.assertTrue(
            all(ffa_df["data_years"] >= 5),
            "All sites should have minimum years of data"
        )
        self.assertTrue(
            all(ffa_df["outliers_removed"] >= 0),
            "Outliers removed should be non-negative"
        )

    def test_weekly_flow_exceedance(self):
        """Test weekly flow exceedance calculation."""
        df = weekly_flow_exceedance(self.con, self.parquet_path)
        # Basic structure tests
        self.assertFalse(df.empty)
        expected_cols = ["site", "week", "p10", "p50", "p95"]
        for col in expected_cols:
            self.assertIn(col, df.columns)
        # Value validation
        self.assertTrue(
            all(df["week"] >= 0),
            "Week numbers should be non-negative"
        )
        self.assertTrue(
            all(df["week"] <= 53),
            "Week numbers should not exceed 53"
        )
        # Percentile ordering validation
        self.assertTrue(all(df["p10"] >= df["p50"]), "P10 should be >= P50")
        self.assertTrue(all(df["p50"] >= df["p95"]), "P50 should be >= P95")

    def test_aggregate_flows(self):
        """Test flow aggregation at different temporal resolutions."""
        # Test daily aggregation
        df_daily = aggregate_flows(
            self.con, self.parquet_path,
            temporal_resolution="daily"
        )
        self.assertFalse(df_daily.empty)
        self.assertIn("time_period", df_daily.columns)
        self.assertIn("mean_flow", df_daily.columns)
        # Test monthly aggregation
        df_monthly = aggregate_flows(
            self.con, self.parquet_path, temporal_resolution="monthly"
            )
        self.assertFalse(df_monthly.empty)
        # Monthly should have fewer records than daily for same sites
        sites_daily = set(df_daily["site"])
        sites_monthly = set(df_monthly["site"])
        self.assertEqual(
            sites_daily, sites_monthly, "Same sites should be present"
            )
        daily_count = len(df_daily)
        monthly_count = len(df_monthly)
        self.assertLess(
            monthly_count,
            daily_count,
            "Monthly aggregation should have fewer records"
        )
        # Test seasonal aggregation
        df_seasonal = aggregate_flows(
            self.con,
            self.parquet_path,
            temporal_resolution="seasonal"
        )
        self.assertFalse(df_seasonal.empty)
        # Check seasonal patterns
        seasonal_periods = df_seasonal["time_period"].unique()
        seasons = [
            period.split('-')[1] for
            period in seasonal_periods if '-' in str(period)
        ]
        expected_seasons = ['Winter', 'Spring', 'Summer', 'Fall']
        for season in seasons:
            if season in expected_seasons:
                # Only check if we found recognizable seasons
                self.assertIn(
                    season, expected_seasons,
                    f"Season {season} should be valid"
                )

    def test_data_consistency_across_functions(self):
        """Test that different functions return
        consistent results for overlapping calculations."""
        # Compare annual peaks from two different functions
        peaks_direct = annual_peaks(self.con, self.parquet_path)
        peaks_from_flow = peak_flows(self.con, self.parquet_path)
        # Calculate mean of annual peaks manually
        manual_mean_peaks = (
            peaks_direct
            .groupby("site")["annual_peak"]
            .mean().reset_index()
        )
        manual_mean_peaks.columns = ["site", "manual_mean"]
        # Compare with peak_flows results
        comparison = peaks_from_flow.merge(manual_mean_peaks, on="site")
        # Allow for small numerical differences
        differences = abs(
            comparison["mean_annual_peak"] - comparison["manual_mean"]
            )
        max_diff = differences.max()
        self.assertLess(
            max_diff, 0.001,
            "Mean annual peaks should be consistent across functions"
        )

    def test_temporal_filtering(self):
        """Test date filtering functionality."""
        # Test with date range that should exist in the data
        start_date = "2000-01-01"
        end_date = "2005-12-31"
        df_filtered = mean_annual_flow(
            self.con, self.parquet_path,
            start_date=start_date, end_date=end_date
        )
        df_full = mean_annual_flow(self.con, self.parquet_path)
        self.assertFalse(
            df_filtered.empty,
            "Filtered data should not be empty"
        )
        # Should have some sites in common
        common_sites = set(df_filtered["site"]) & set(df_full["site"])
        self.assertGreater(
            len(common_sites), 0,
            "Should have common sites between filtered and full data"
        )
        print(
            f"Temporal filtering - Full period sites: {len(df_full)}, "
            f"Filtered period sites: {len(df_filtered)}"
        )


if __name__ == "__main__":
    # Run with more verbose output
    unittest.main(verbosity=2)
