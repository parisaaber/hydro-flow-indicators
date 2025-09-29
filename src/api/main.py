from fastapi import FastAPI
from starlette.middleware.gzip import GZipMiddleware
from api.routers import (
    etl_router,
    indicators_router,
    mapping_router,
    connection_router,
)
from api import __version__ as VERSION


app = FastAPI(
    title="Raven Model API",
    description="Abstract Raven model outputs and collect flow indicators.",
    version=VERSION,
)

app.add_middleware(GZipMiddleware, minimum_size=1024)

app.include_router(connection_router, prefix="/api/connection", tags=["Connection"])
app.include_router(etl_router, prefix="/api/etl", tags=["ETL"])
app.include_router(indicators_router, prefix="/api/indicators", tags=["Indicators"])
app.include_router(mapping_router, prefix="/api/mapping", tags=["Mapping"])
