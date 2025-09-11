# 🌊 Raven Streamflow Indicators API

This FastAPI-based application provides a web service for computing a suite of hydrological indicators from Raven hydrologic model output or any comparable streamflow time series. It supports environmental flow metrics, flood frequency analysis, sub-period comparisons, and **site-specific filtering** via a flexible and powerful API.

## 📦 Features

- **Flexible Data Source**: Load and preprocess streamflow datasets from local Parquet files or remote URLs.
- **Advanced Filtering**: Filter results by date range and one or more `site` IDs.
- **Comprehensive Hydrologic Indicators**:
  - Mean annual flow (overall or annual)
  - Mean August–September flow (overall or annual)
  - Peak flow timing, overall average or per year (Day of Year)
  - Days below Environmental Flow Needs (EFN) threshold (overall or annual)
  - Annual peak flows
  - Mean annual peak flow
  - Weekly flow exceedance thresholds (P05, P10, ..., P95)
  - **Enhanced Flood Frequency Analysis (FFA)** with multiple distributions (Gumbel, Log-Pearson III, Gamma, etc.), automatic best-fit selection, and outlier detection.
- **Temporal Aggregation**: Retrieve daily, weekly, monthly, or seasonal hydrographs.
- **Sub-period Analysis**: Compare indicators before and after a specified break point year.
- **Built for Scale**: Leverages DuckDB for fast querying and processing of large datasets.
- **RESTful API**: Fully documented with OpenAPI and Swagger UI.

## 🛠 Installation

### System

```bash
# Clone the repo
git clone https://github.com/parisaaber/hydro-flow-indicators.git
cd hydro-flow-indicators

# (Optional) create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

# Install dependencies
python -m pip install -e .

# (Optional) Run the tests
python -m unittest -v tests/test_indicators_with_real_data.py

# Run the API
uvicorn src.api.main:app --reload

# To visit the docs
http://127.0.0.1:8000/docs

# Once done, deactivate and remove the virtual environment
deactivate
rm -rf venv
```

### Using Docker

Alternatively, run the app using Docker.

```Bash
# Build the image with the name hfi
docker build -t hfi -f Dockerfile .

# Run the tests
docker run --rm -it \
  --entrypoint /bin/sh \
  -v "$HOME/.aws:/root/.aws" \
  -v "$(pwd)/tests:/tests" \
  hfi

python -m unittest discover -s /tests

# Run the API
docker run --rm -it -p 8000:8000 \
  --entrypoint "" \
  -e PYTHONPATH="/var/task/py:/app" \
  -v "$(pwd)/src:/app/src" \
  --workdir /app \
  hfi \
  uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# To visit the docs
http://0.0.0.0:8000/docs

```

## 🔌 API Endpoints

### `POST /etl/init`

Initialize the ETL process to convert a Raven output CSV to the internal Parquet format.

- **Body Parameters**: `csv_path`, `output_path`

### `GET /indicators/sites`

List all available site names in a Parquet file.

- **Query Parameter**: `parquet_src` (required)

## 🧮 Indicator Calculation Endpoints

All indicator endpoints accept these common query parameters for filtering:

- `parquet_src` **(str, required)**: Full path or URL to a Parquet file.
- `sites` **(List[str], optional)**: Filter results for specific sites (e.g., `sites=site1&sites=site2`).
- `start_date` **(str, optional)**: Start date for filtering (`YYYY-MM-DD`).
- `end_date` **(str, optional)**: End date for filtering (`YYYY-MM-DD`).

| Endpoint                                       | Description                                                            | Specific Parameters                                                                                                   |
| :--------------------------------------------- | :--------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| **`GET /indicators/`**                         | Compute **all indicators** for the specified period or subperiods.     | `efn_threshold`, `break_point`                                                                                        |
| **`GET /indicators/mean_annual_flow`**         | Mean annual flow.                                                      | `temporal_resolution` (`overall` or `annual`)                                                                         |
| **`GET /indicators/mean_aug_sep_flow`**        | Mean August–September flow.                                            | `temporal_resolution` (`overall` or `annual`)                                                                         |
| **`GET /indicators/peak_flow_timing`**         | Average day of year of annual peak flow.                               | `temporal_resolution` (`overall` or `annual`)                                                                         |
| **`GET /indicators/days_below_efn`**           | Number of days below the Environmental Flow Needs threshold.           | `efn_threshold`, `temporal_resolution` (`overall` or `annual`)                                                        |
| **`GET /indicators/annual_peaks`**             | Annual peak flows for each water year.                                 | -                                                                                                                     |
| **`GET /indicators/peak_flows`**               | Mean annual peak flow.                                                 | -                                                                                                                     |
| **`GET /indicators/weekly_flow_exceedance`**   | Weekly flow exceedance probabilities (P05 to P95).                     | -                                                                                                                     |
| **`GET /indicators/flood_frequency_analysis`** | **Enhanced FFA** with configurable distributions and outlier handling. | `return_periods`, `dist`, `remove_outliers`, `outlier_method`, `outlier_threshold`, `min_years`, `selection_criteria` |
| **`GET /indicators/aggregate_flows`**          | Get a hydrograph aggregated to a specified time resolution.            | `temporal_resolution` (`daily`, `weekly`, `monthly`, `seasonal`)                                                      |

## 📂 Project Structure

```
hydro-flow-indicators/
├── src/
│   ├── raven_api/
│   │   ├── etl.py           # ETL process to convert CSV to Parquet
│   │   ├── indicators.py    # Core indicator calculation logic
│   │   ├── utils.py         # Helper functions (FFA, outlier detection)
│   │   └── __init__.py
│   └── api/
│       ├── __init__.py
│       ├── main.py          # FastAPI application factory
│       └── routers.py       # API route definitions (this file)
├── tests/
│   └── test_data/           # Directory for sample test data
└── README.md
```

## 📈 Input Data Format

The API expects Parquet files with the following schema:

- `date` **(date)**: The date of the observation.
- `site` **(str)**: A unique identifier for the gauge/site (e.g., `"sub11004314 [m3/s]"`).
- `value` **(float)**: The streamflow value, ideally in m³/s.

**Note**: The ETL endpoint (`/etl/init`) is provided to convert from Raven's CSV output to this required Parquet format.

## 💡 Example Usage

### 1. Get a list of all available sites

```bash
curl "http://127.0.0.1:8000/indicators/sites?parquet_src=/path/to/data.parquet"
```

### 2. Calculate all indicators for two sites, pre and post-2010

```bash
curl "http://127.0.0.1:8000/indicators/?\
parquet_src=/path/to/data.parquet\
&sites=sub11004314 [m3/s]\
&sites=sub11004315 [m3/s]\
&efn_threshold=0.2\
&break_point=2010"
```

### 3. Perform a Flood Frequency Analysis for a 100-year event using Log-Pearson III

```bash
curl "http://127.0.0.1:8000/indicators/flood_frequency_analysis?\
parquet_src=/path/to/data.parquet\
&sites=sub11004314 [m3/s]\
&return_periods=2,20,100\
&dist=logpearson3\
&remove_outliers=true\
&outlier_method=iqr"
```

### 4. Retrieve the daily aggregated hydrograph for a site

```bash
curl "http://127.0.0.1:8000/indicators/aggregate_flows?\
parquet_src=/path/to/data.parquet\
&sites=sub11004314 [m3/s]\
&temporal_resolution=daily\
&start_date=2010-01-01\
&end_date=2010-12-31"
```

## 🪪 License
