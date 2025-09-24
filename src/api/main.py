from fastapi import FastAPI
from api.routers import (
    etl_router,
    indicators_router,
    mapping_router,
    connection_router,
)
from api import __version__ as VERSION


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

app.include_router(connection_router, prefix="/api/connection", tags=["Connection"])
app.include_router(etl_router, prefix="/api/etl", tags=["ETL"])
app.include_router(indicators_router, prefix="/api/indicators", tags=["Indicators"])
app.include_router(mapping_router, prefix="/api/mapping", tags=["Mapping"])
