from fastapi import FastAPI

from api.routers import etl_router, indicators_router
from api import __version__ as VERSION


app = FastAPI(
    title="Raven Model API",
    description="""
    Abstract Raven model outputs and collect flow indicators
    """,
    version=VERSION,
)

app.include_router(etl_router, prefix="/etl", tags=["ETL"])
app.include_router(indicators_router, prefix="/indicators", tags=["Indicators"])
