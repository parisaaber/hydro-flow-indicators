# 🌊 Raven Streamflow Indicators API

This FastAPI-based application provides a web service for computing a suite of hydrological indicators from Raven hydrologic model output or any comparable streamflow time series. It supports environmental flow metrics, flood frequency analysis, sub-period comparisons, and now **site-specific filtering** and **individual indicator endpoints**.

---

## 📦 Features

* Load and preprocess Raven output or streamflow datasets from local files or web URLs
* Filter results by one or more `site` IDs
* Calculate hydrologic indicators:

  * Mean annual flow
  * Mean August–September flow
  * Peak flow timing
  * Days below EFN threshold
  * Annual peak flow and flood quantiles (Gumbel)
* Support for sub-period (e.g., pre/post intervention) comparisons
* Built-in flood frequency analysis for return periods (e.g., Q2, Q20)
* Exposed via RESTful API using FastAPI

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

| Endpoint                  | Description                            |
| ------------------------- | -------------------------------------- |
| `GET /mean-annual-flow/`  | Mean annual flow                       |
| `GET /mean-aug-sep-flow/` | Mean August–September flow             |
| `GET /peak-flow-timing/`  | Average peak flow timing (day of year) |
| `GET /days-below-efn/`    | Days below EFN threshold               |
| `GET /peak-flows/`        | Mean annual peak flows                 |
| `GET /annual-peaks/`      | Annual peak flows per water year       |
| `GET /fit-ffa/`           | Flood Frequency Analysis (Gumbel)      |

Each accepts:

* `csv_path` *(str, required)*
* Additional params like `efn_threshold`, `sites` (comma-separated), etc.

---

## 📂 Project Structure

```plaintext
hydro-flow-indicators/
├── src/
│   ├── raven_api/
│   │   ├── app.py           # FastAPI app and routes
│   │   ├── etl.py           # Preprocessing and data loading
│   │   ├── indicators.py    # Indicator calculation logic
│   │   ├── utils.py         # Utility functions (reserved)
│   │   └── __init__.py      # Package init
│   └── api/
│       ├── __init__.py
│       ├── main.py          # FastAPI entrypoint
│       └── routers.py       # API route definitions
├── tests/
│   └── test_data/
│       └── Hydrographs.csv.gz  # Sample test data (compressed)
├── requirements.txt
└── README.md
```

---

## 📈 Input Data Format

Input files should contain:

* `date`: datetime string (or `time`)
* `site`: site/station ID
* `value`: flow in m³/s

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
