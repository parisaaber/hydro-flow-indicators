# 🌊 Raven Streamflow Indicators API

This FastAPI-based application provides a web service for computing a suite of hydrological indicators from Raven hydrologic model output or any comparable streamflow time series in Parquet format. It supports environmental flow metrics, flood frequency analysis, and sub-period comparisons.

## 📦 Features

- Load and preprocess Raven output or streamflow datasets
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
```

---

## 🚀 Running the API

```bash
uvicorn app:app --reload
```

Then open your browser to:
[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) for the interactive Swagger UI.

---

## 🔌 API Endpoints

### `GET /`

Basic welcome message.

---

### `GET /indicators/`

Compute flow indicators from a Parquet file.

#### Query Parameters:

* `parquet_path` *(str, required)*: Path to the input Parquet file
* `efn_threshold` *(float, optional, default=0.2)*: EFN threshold as a fraction of mean annual flow
* `break_point` *(int, optional)*: Optional water year to split into subperiods

#### Example:

```
/indicators/?parquet_path=data/flow_data.parquet&efn_threshold=0.2&break_point=2005
```

#### Response:

A list of dictionaries, each containing flow indicators for a site and sub-period.

---

## 📂 Project Structure

```text
raven-streamflow-api/
├── app.py                  # FastAPI entry point
├── raven_api/
│   ├── etl.py              # Data loading, reshaping, and preprocessing
│   ├── indicators.py       # Indicator calculation logic
│   ├── utils.py            # (Empty, reserved for future use)
│   ├── __init__.py         # Marks as package
├── requirements.txt        # Dependencies
└── README.md               # Project documentation
```

---

## 📈 Input Data Format

Input files should be in **Parquet** format with at least the following columns:

* `date`: datetime of observation
* `site`: name or ID of the location
* `value`: streamflow or discharge value

---

## 📤 Example Workflow

1. Convert your CSV Raven output to long-format and save as Parquet using the helper:

   ```python
   from raven_api.etl import load_raven_output, reshape_to_long, save_to_parquet

   df = load_raven_output("Results.csv")
   long_df = reshape_to_long(df)
   save_to_parquet(long_df, "flow_data.parquet")
   ```

2. Run the API and query indicators using:

   ```
   http://127.0.0.1:8000/indicators/?parquet_path=flow_data.parquet&break_point=2005
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
