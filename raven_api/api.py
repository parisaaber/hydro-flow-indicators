from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from tempfile import NamedTemporaryFile
from urllib.parse import unquote
import shutil
import os
import requests  # For fetching remote files

from raven_api.etl import load_raven_output, reshape_to_long, save_to_parquet
from raven_api.indicators import calculate_all_indicators

app = FastAPI(title="Raven API", version="0.3")  # Updated version


@app.get("/")
def read_root():
    return {"message": "Welcome to the Raven API. Use /indicators/, /indicators-local/, or /indicators-url/ endpoint to calculate flow indicators."}


@app.post("/indicators/")
async def get_indicators_from_csv(file: UploadFile = File(...),
                                  efn_threshold: float = 0.2,
                                  break_point: int = None):
    """
    Upload a Raven CSV file and compute flow indicators.

    Args:
        file: Raven output CSV file.
        efn_threshold: EFN threshold (e.g., 0.2).
        break_point: Optional year to divide data into subperiods.

    Returns:
        List of dictionaries with calculated indicators.
    """
    try:
        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp_csv:
            shutil.copyfileobj(file.file, tmp_csv)
            csv_path = tmp_csv.name

        df = load_raven_output(csv_path)
        long_df = reshape_to_long(df)

        with NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_parquet:
            parquet_path = tmp_parquet.name
            save_to_parquet(long_df, parquet_path)

        result_df = calculate_all_indicators(parquet_path, efn_threshold, break_point)
        return result_df.to_dict(orient='records')

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        try:
            os.remove(csv_path)
            os.remove(parquet_path)
        except:
            pass


@app.get("/indicators-local/")
def get_indicators_from_path(csv_path: str = Query(..., description="Local CSV file path on server"),
                             efn_threshold: float = 0.2,
                             break_point: int = None):
    """
    Compute indicators from a local Raven CSV file path.
    This is for development purposes only.
    """
    try:
        decoded_path = unquote(csv_path)

        df = load_raven_output(decoded_path)
        long_df = reshape_to_long(df)

        with NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_parquet:
            parquet_path = tmp_parquet.name
            save_to_parquet(long_df, parquet_path)

        result_df = calculate_all_indicators(parquet_path, efn_threshold, break_point)
        return result_df.to_dict(orient='records')

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        try:
            os.remove(parquet_path)
        except:
            pass


@app.get("/indicators-url/")
def get_indicators_from_url(csv_url: str = Query(..., description="URL to CSV file"),
                            efn_threshold: float = 0.2,
                            break_point: int = None):
    """
    Fetch a Raven CSV file from a web URL and compute flow indicators.
    """
    try:
        # Download the CSV file from URL
        response = requests.get(csv_url, stream=True)
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail="Could not download CSV file from URL")

        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp_csv:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp_csv.write(chunk)
            csv_path = tmp_csv.name

        df = load_raven_output(csv_path)
        long_df = reshape_to_long(df)

        with NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_parquet:
            parquet_path = tmp_parquet.name
            save_to_parquet(long_df, parquet_path)

        result_df = calculate_all_indicators(parquet_path, efn_threshold, break_point)
        return result_df.to_dict(orient='records')

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        try:
            os.remove(csv_path)
            os.remove(parquet_path)
        except:
            pass