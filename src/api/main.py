from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routers import (
    etl_router,
    indicators_router,
    mapping_router,
    connection_router,
)
from src.api import __version__ as VERSION


app = FastAPI(
    title="Raven Model API",
    description="""
    Abstract Raven model outputs and collect flow indicators.
    ## Connection Management
    This API uses a global DuckDB connection for optimal performance. You can:
    1. **Configure** the connection with a parquet file
    and filters using `/connection/configure`
    2. **Check status** of the current connection using `/connection/status`
    3. **Reset** the connection configuration using `/connection/reset`
    ## Performance Optimization
    The global connection creates reusable database
    views for common operations:
    - `main_data`: Raw data from parquet file
    - `mean_annual_flows`: Pre-computed mean annual flows per site
    - `annual_maximums`: Pre-computed annual maximum flows
    - `daily_statistics`: Enhanced daily data with date components
    Multiple queries will reuse these views for improved performance.
    """,
    version=VERSION,
)

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    connection_router, prefix="/connection", tags=["Connection"]
    )
app.include_router(
    etl_router, prefix="/etl", tags=["ETL"]
    )
app.include_router(
    indicators_router, prefix="/indicators", tags=["Indicators"]
    )
app.include_router(
    mapping_router, prefix="/mapping", tags=["Mapping"]
    )
