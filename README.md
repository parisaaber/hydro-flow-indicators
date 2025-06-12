# 🌊 Raven Streamflow Indicators API

This FastAPI-based application provides a web service for computing a suite of hydrological indicators from Raven hydrologic model output or any comparable streamflow time series in CSV format. It supports environmental flow metrics, flood frequency analysis, and sub-period comparisons.

## 📦 Features

- Load and preprocess Raven output or streamflow datasets from local files or web URLs
- Calculate hydrologic indicators:
  - Mean annual flow
  - Mean August–September flow
  - Peak flow timing
  - Days below EFN threshold
  - Annual peak flow and flood quantiles (Gumbel)
- Support for sub-period (e.g., pre/post intervention) comparisons
- Built-in flood frequency analysis for return periods (e.g., Q2, Q20)
- Exposed via RESTful API using FastAPI

---

## 🛠 Installation

```bash
# Clone the repo
git clone https://github.com/your-username/raven-streamflow-api.git
cd raven-streamflow-api

# (Optional) create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

# Install dependencies
pip install -r requirements.txt
````

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
uvicorn raven_api.app:app --reload
```

Then open your browser to:
[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) for the interactive Swagger UI.

---

## 🔌 API Endpoints

### `GET /`

Basic welcome message.

---

### `POST /indicators/`

Upload a Raven CSV file and compute flow indicators.

#### Form Data:

* `file` *(file, required)*: Upload a Raven output CSV file.
* `efn_threshold` *(float, optional, default=0.2)*: EFN threshold as a fraction of mean annual flow
* `break_point` *(int, optional)*: Optional water year to split into subperiods

#### Example curl:

```bash
curl -X 'POST' \
  'http://127.0.0.1:8000/indicators/?efn_threshold=0.2&break_point=2005' \
  -H 'accept: application/json' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@Hydrographs.csv;type=text/csv'
```

---

### `GET /indicators-local/`

Compute indicators from a local Raven CSV file path or web URL.

#### Query Parameters:

* `csv_path` *(str, required)*: Local file path or web URL to the Raven CSV file
* `efn_threshold` *(float, optional, default=0.2)*: EFN threshold as a fraction of mean annual flow
* `break_point` *(int, optional)*: Optional water year to split into subperiods

#### Example:

```
http://127.0.0.1:8000/indicators-local/?csv_path=https://github.com/parisaaber/HydroFlowIndicators/raw/refs/heads/main/Hydrographs.csv&efn_threshold=0.2&break_point=2005
```

---

## 📂 Project Structure

```text
HydroFlowIndicators/
├── raven_api/
│   ├── app.py              # FastAPI entry point
│   ├── etl.py              # Data loading, reshaping, and preprocessing
│   ├── indicators.py       # Indicator calculation logic
│   ├── utils.py            # (Empty, reserved for future use)
│   ├── __init__.py         # Marks as package
└── README.md               # Project documentation
```

---

## 📈 Input Data Format

Input CSV files should have columns like:

* `time`: datetime or timestamp of observation
* `date`: date of observation
* `hour`: hour of day
* `precip [mm/day]`: precipitation amount
* `sub_xxx1 [m3/s]`, `sub_xxx2 [m3/s]`, ..., `sub_xxxN [m3/s]`: streamflow values for subbasins

Example columns:

```
time, date, hour, precip [mm/day], sub_xxx1 [m3/s], sub_xxx2 [m3/s], ..., sub_xxxN [m3/s]
```

---

## 📤 Example Workflow

1. Use the sample Raven CSV file hosted online:

```
https://github.com/parisaaber/HydroFlowIndicators/raw/refs/heads/main/Hydrographs.csv
```

2. Query indicators by sending a GET request to the `/indicators-local/` endpoint:

```
http://127.0.0.1:8000/indicators-local/?csv_path=https://github.com/parisaaber/HydroFlowIndicators/raw/refs/heads/main/Hydrographs.csv&efn_threshold=0.2&break_point=2005
```

---

## 🤝 Contributing

Contributions, issues, and suggestions are welcome! Feel free to open a pull request or issue if you'd like to improve or extend this project.

---

## 🪪 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

---

## 📬 Contact

For questions or collaboration:
📧 [parisa.aberi@machydro.ca](mailto:parisa.aberi@machydro.ca)
