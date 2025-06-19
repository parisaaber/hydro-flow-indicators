# Unit Tests for Hydrologic Indicators

This repository contains unit tests for various hydrologic indicator functions used in the `raven_api.indicators` module. The tests are written using Python’s built-in `unittest` framework.

## Tested Indicators

- `mean_annual_flow`
- `mean_aug_sep_flow`
- `peak_flow_timing`
- `days_below_efn`
- `peak_flows`
- `annual_peaks`
- `fit_ffa`

## Sample Data

The test suite uses a small synthetic dataset of daily streamflow values across two water years for a single site (`site='A'`). The dataset includes date, value, site, and calculated water year.

## Running the Tests

To run all the unit tests:

```bash
python test_indicators.py
````

Make sure you have installed `pandas` and have access to the `raven_api` package in your Python environment.

## Dependencies

* Python 3.7+
* `pandas`
* `raven_api` (must include the `indicators` module)


