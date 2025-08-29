from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routers import etl_router, indicators_router
from src.api import __version__ as VERSION


app = FastAPI(
    title="Raven Model API",
    description="""
    Abstract Raven model outputs and collect flow indicators
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

app.include_router(etl_router, prefix="/etl", tags=["ETL"])
app.include_router(indicators_router, prefix="/indicators", tags=["Indicators"])
