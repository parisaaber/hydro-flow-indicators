# Unit Tests for Hydrologic Indicators

This repository contains unit tests for various hydrologic indicator functions implemented in the `raven_api.indicators` module. The tests are written using Python’s built-in `unittest` framework and validate the indicator calculations against real-world streamflow data.

## Tested Indicators

* `mean_annual_flow`
* `mean_aug_sep_flow`
* `peak_flow_timing`
* `days_below_efn`
* `peak_flows`
* `annual_peaks`
* `fit_ffa`

## Test Data

The tests download a compressed CSV dataset (`Hydrographs.csv.gz`) from the repository, which contains daily streamflow data for multiple sites over several years. This data is processed into a long format and saved temporarily as a Parquet file for use in the tests.

## Testing Approach

* The data is loaded and transformed to include water year calculations.
* Tests establish a DuckDB in-memory connection to efficiently query the Parquet dataset.
* Each hydrologic indicator function is run against this data.
* Assertions verify that results are non-empty and contain expected columns.

## Running the Tests

To run all unit tests:

```bash
python -m unittest test_indicators.py
```

Or simply:

```bash
python test_indicators.py
```

Make sure your environment includes all dependencies.

## Dependencies

* Python 3.7+
* `pandas`
* `duckdb`
* `requests`
* `raven_api` (must include the `indicators` module)
* `pyarrow` or `fastparquet` (for Parquet support via pandas)

## Notes

* The test suite requires internet access to download the test dataset on the first run.
* Temporary files are cleaned up automatically after tests complete.
* The tests check that the indicator outputs contain expected metric columns such as `"mean_annual_flow"`, `"days_below_efn"`, `"Q2"`, `"Q20"`, etc.

