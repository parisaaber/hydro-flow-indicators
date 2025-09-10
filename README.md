# 🌊 Raven Streamflow Indicators API

This FastAPI-based application provides a web service for computing a suite of hydrological indicators from Raven hydrologic model output or any comparable streamflow time series. It supports environmental flow metrics, flood frequency analysis, sub-period comparisons, and site-specific filtering via a flexible and powerful API.

---

## ✨ Features

- **📊 Multiple Data Sources**: Load from local Parquet files or remote URLs
- **🎯 Advanced Filtering**: Filter by date range and multiple site IDs
- **📈 Comprehensive Indicators**:
  - Mean annual & seasonal flows
  - Peak flow timing (DOY)
  - Environmental Flow Needs (EFN) analysis
  - Annual peak flows and flood quantiles
  - Weekly flow exceedance thresholds
- **⚡ High Performance**: Powered by DuckDB for fast data processing
- **🔬 Enhanced FFA**: Multiple distributions (Gumbel, Log-Pearson III, Gamma, etc.) with automatic best-fit selection
- **⏰ Temporal Aggregation**: Daily, weekly, monthly, and seasonal hydrographs
- **📊 Sub-period Analysis**: Compare pre/post intervention periods
- **📚 Fully Documented**: Interactive OpenAPI/Swagger documentation

---

## 🛠 Installation

```bash
# Clone the repo
git clone https://github.com/parisaaber/hydro-flow-indicators.git
cd hydro-flow-indicators

# (Optional) create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

# Install dependencies
pip install -r requirements.txt
```

### Example `requirements.txt`

```txt
fastapi
uvicorn
pandas
numpy
duckdb
scipy
requests
```

---

## 🚀 Running the API

```bash
uvicorn src.api.main:app --reload
```

Then visit:

[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) — interactive Swagger UI

---

## 🔌 API Endpoints

### `GET /`

Basic welcome message.

---

### `POST /indicators/`

Upload a Raven CSV or Parquet file and compute all indicators.

#### Form Data:

* `file` *(file, required)*: Raven output CSV or Parquet file.
* `efn_threshold` *(float, optional, default=0.2)*: EFN threshold as a fraction of mean annual flow (MAF).
* `break_point` *(int, optional)*: Water year to split into subperiods.
* `sites` *(comma-separated list, optional)*: Filter by one or more site IDs.

---

### `GET /indicators-local/`

Compute indicators from a **local file path** or **web URL**.

#### Query Parameters:

* `csv_path` *(str, required)*: Path or URL to Raven CSV/Parquet file.
* `efn_threshold` *(float, optional, default=0.2)*
* `break_point` *(int, optional)*
* `sites` *(comma-separated string, optional)*

---

### 🔍 Individual Indicator Endpoints

Fetch **specific indicators** (optionally filtered by site):

Endpoint	Description	Special Parameters
/indicators/	Compute all indicators	efn_threshold, break_point
/indicators/mean_annual_flow	Mean annual flow	temporal_resolution
/indicators/mean_aug_sep_flow	Aug-Sept mean flow	temporal_resolution
/indicators/peak_flow_timing	Peak flow timing (DOY)	temporal_resolution
/indicators/days_below_efn	Days below EFN threshold	efn_threshold, temporal_resolution
/indicators/annual_peaks	Annual peak flows per water year	-
/indicators/peak_flows	Mean annual peak flow	-
/indicators/weekly_flow_exceedance	Weekly exceedance probabilities	-
/indicators/flood_frequency_analysis	Enhanced FFA	return_periods, dist, remove_outliers, etc.
/indicators/aggregate_flows	Aggregated hydrograph	temporal_resolution


Each accepts:

* `csv_path` *(str, required)*
* Additional params like `efn_threshold`, `sites` (comma-separated), etc.

---

## 📂 Project Structure

```plaintext
raven-streamflow-indicators/
├── src/
│   ├── raven_api/
│   │   ├── etl.py           # CSV to Parquet conversion
│   │   ├── indicators.py    # Core indicator calculations
│   │   ├── utils.py         # FFA and helper functions
│   │   └── __init__.py
│   └── api/
│       ├── __init__.py
│       ├── main.py          # FastAPI application
│       └── routers.py       # API route definitions
├── tests/
│   └── test_data/           # Sample test data
├── requirements.txt
└── README.md
```

---

## 📈 Input Data Format

Input files should contain:

* `date`: datetime string (or `time`)
* `site`: site/station ID
* `value`: flow in m³/s
Use /etl/init to convert Raven CSV output to this format.

📝 Supported formats: CSV or Parquet

Example CSV snippet:

```csv
date,site,value
2020-01-01,sub_xxx1,1.23
2020-01-02,sub_xxx1,1.12
...
```

You can find sample data here:

[Hydrographs.csv.gz](https://github.com/parisaaber/hydro-flow-indicators/blob/main/tests/test_data/Hydrographs.csv.gz)

---

## 💡 Example Workflow

### Compute all indicators for remote data with site filtering and subperiod:

```
http://127.0.0.1:8000/indicators-local/?csv_path=https://github.com/parisaaber/hydro-flow-indicators/raw/main/tests/test_data/Hydrographs.csv.gz&efn_threshold=0.2&break_point=2005&sites=sub_xxx1,sub_xxx2
```

### Fetch only mean annual flow for site `sub_xxx1`:

```
http://127.0.0.1:8000/mean-annual-flow/?csv_path=https://github.com/parisaaber/hydro-flow-indicators/raw/main/tests/test_data/Hydrographs.csv.gz&sites=sub_xxx1
```

---

## 🪪 License

MIT License. See [LICENSE](LICENSE) for full terms.
