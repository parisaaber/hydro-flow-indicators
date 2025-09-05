from __future__ import annotations
from typing import Iterable, Optional, Dict, Any, List, Iterator
import os
import requests
from urllib.parse import urlsplit
from contextlib import contextmanager
from tempfile import TemporaryDirectory
import geopandas as gpd
import pandas as pd
from shapely.geometry import mapping as shapely_mapping
from pyproj import CRS


def _is_remote(path: str) -> bool:
    return path.lower().startswith(("http://", "https://"))


@contextmanager
def _localize_if_remote(geoparquet_src: str) -> Iterator[str]:
    if not _is_remote(geoparquet_src):
        yield geoparquet_src
        return

    tmp_dir = TemporaryDirectory()
    try:
        filename = os.path.basename(urlsplit(geoparquet_src).path) or "features.parquet"
        local_path = os.path.join(tmp_dir.name, filename)

        with requests.get(geoparquet_src, stream=True, timeout=30) as resp:
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1_048_576):
                    if chunk:
                        f.write(chunk)

        yield local_path
    finally:
        tmp_dir.cleanup()


def _to_feature_collection(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    props_df = gdf.drop(columns=gdf.geometry.name)
    props_df = props_df.applymap(
        lambda v: (
            v.isoformat()
            if isinstance(v, pd.Timestamp)
            else (v.item() if hasattr(v, "item") else v)
        )
    )

    features: List[Dict[str, Any]] = []
    for (idx, row), geom in zip(props_df.iterrows(), gdf.geometry):
        features.append(
            {
                "type": "Feature",
                "properties": row.to_dict(),
                "geometry": shapely_mapping(geom) if geom is not None else None,
            }
        )

    return {"type": "FeatureCollection", "features": features}


def map_features(
    geoparquet_src: str,
    sites: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    with _localize_if_remote(geoparquet_src) as local_path:
        gdf = gpd.read_parquet(local_path)

    if "site" not in gdf.columns:
        raise ValueError(
            "GeoParquet must contain a 'site' column (renamed during export)."
        )
    if gdf.geometry.name is None:
        raise ValueError("GeoParquet is missing a geometry column.")

    if sites:
        gdf = gdf[gdf["site"].isin(list(sites))]

    if gdf.empty:
        return {"type": "FeatureCollection", "features": []}

    if gdf.crs is not None:
        crs_4326 = CRS.from_epsg(4326)
        try:
            if not CRS.from_user_input(gdf.crs).equals(crs_4326):
                gdf = gdf.to_crs(crs_4326)
        except Exception:
            # if CRS is unparsable, assume it's already 4326
            pass

    return _to_feature_collection(gdf)
