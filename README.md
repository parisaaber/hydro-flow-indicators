# HydroFlowIndicators

This repository provides a streamlined workflow for computing hydrologic flow indicators from model-generated or observed streamflow time series (e.g., Raven hydrologic model output). It includes tools for reading, reshaping, converting to Parquet, and extracting key indicators such as peak flows, flow timing, and ecological flow thresholds.

---

## 📦 Features

- Load hydrological model output from CSV (Raven format).
- Reshape data into long-format suitable for analysis.
- Save streamflow time series as an efficient `.parquet` file.
- Compute various hydrological indicators:
  - Mean annual flow
  - Mean flow during Aug–Sep
  - Peak flow timing (Julian day of maximum)
  - Days below Environmental Flow Needs (EFN) threshold
  - 2-year and 20-year peak flow estimates using Gumbel distribution

---

## 🔧 Dependencies

- `pandas`
- `numpy`
- `duckdb`
- `scipy`

Install with:

```bash
pip install pandas numpy duckdb scipy
````

---

## 🚀 Usage Example (e.g., in Colab)

```python
import requests

# Dynamically load the script from GitHub
exec(__import__("requests").get(
    "https://raw.githubusercontent.com/parisaaber/HydroFlowIndicators/main/script.py"
).text)

# Define paths
csv_path = "https://github.com/parisaaber/HydroFlowIndicators/raw/refs/heads/main/Hydrographs.csv"
out_path = "/content/Hydrographs.parquet"

# Process and analyze data
df_long = reshape_to_long(load_raven_output(csv_path))
save_to_parquet(df_long, out_path)
indicators = calculate_indicators(out_path)

# Show result
print(indicators)
```

---

## 📁 Input File Format

The input `.csv` file should contain:

* A `date` column (interpretable by `pandas.to_datetime`)
* Multiple flow columns (e.g., `site_01`, `site_02`, ...) without "observed" in their name
* Tab or comma delimiters

Example:

```
date,time,hour,site_01,site_02,...
1990-01-01,00:00,0,12.4,3.2,...
```

---

## 📤 Output

The workflow generates:

1. A **Parquet file** with reshaped streamflow data:

   * Path: `/content/Hydrographs.parquet` (or as specified in `out_path`)
   * Structure: long-format with columns like `date`, `site`, `flow`

2. A **DataFrame** with hydrological indicators:

   * Columns:

| Column            | Description                                  |
| ----------------- | -------------------------------------------- |
| site              | Site ID or name                              |
| subperiod         | Time period split (e.g., full, before/after) |
| Days below EFN    | Avg. days/year below environmental flow      |
| 2-Year Peak Flow  | 2-year return period flow                    |
| Mean Annual Flow  | Average annual flow                          |
| Mean Aug-Sep Flow | Average flow in August–September             |
| Peak Flow Timing  | Julian day of annual peak flow               |
| 20-Year Peak Flow | 20-year return period flow                   |

---

## 📘 License

MIT License

---

## 👩‍💻 Author

Developed by [Parisa Aberi](https://github.com/parisaaber)

```
