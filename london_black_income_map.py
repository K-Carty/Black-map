"""
Interactive choropleth map of **London MSOAs** showing:

• **Share of residents identifying as Black** (Census 2021, dataset *TS021*)
• **Mean equivalised disposable household income – before housing costs** (ONS small‑area model‑based estimates, FYE 2020)

## 2025‑06‑11 — Version 1.3.5
Modified to work with local data files and added better error handling.

Run:
```bash
python -m venv venv && source venv/bin/activate
pip install --upgrade pip
pip install pandas geopandas shapely requests folium tqdm openpyxl
python london_black_income_map.py               # builds london_black_income_map.html
```
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from textwrap import dedent
from typing import Optional

import geopandas as gpd
import pandas as pd
import requests
from folium import Choropleth, LayerControl, Map, Popup, Icon, Marker, CircleMarker, FeatureGroup, Tooltip, GeoJson, GeoJsonPopup, GeoJsonTooltip, DivIcon
from folium.plugins import MarkerCluster
from shapely.geometry import Point, Polygon
import json

# ---------------------------------------------------------------------------
# Logging & constants
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
LOGGER = logging.getLogger("london-map")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# File paths
BOUNDARIES_ZIP = DATA_DIR / "london_boundaries.zip"
INCOME_XLSX = DATA_DIR / "income_fye2020.xlsx"
BLACK_POP_CSV = DATA_DIR / "black_population.csv"
HOUSING_XLSX = DATA_DIR / "housingMSOA140624.xlsx"

# Dataset identifiers
ONS_INCOME_DATASET = "smallareaincomeestimatesformiddlelayersuperoutputareasenglandandwales"
ONS_INCOME_EDITION = "financial-year-ending-2020"
ONS_INCOME_VERSION = 1

CENSUS_DATASET_ID = "TS021"
CENSUS_VERSION = 1
BLACK_CATEGORY_CODE = "black_black_british_caribbean_or_african"

DEFAULT_TIMEOUT = 60
MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def check_data_files() -> bool:
    """Check if all required data files exist."""
    missing = []
    for file in [BOUNDARIES_ZIP, INCOME_XLSX, BLACK_POP_CSV, HOUSING_XLSX]:
        if not file.exists():
            missing.append(file.name)
    
    if missing:
        LOGGER.error("Missing required data files: %s", ", ".join(missing))
        LOGGER.info("Please download the required files:")
        LOGGER.info("1. London MSOA boundaries: https://data.london.gov.uk/dataset/statistical-gis-boundary-files-london")
        LOGGER.info("2. Income data: https://www.ons.gov.uk/datasets/smallareaincomeestimatesformiddlelayersuperoutputareasenglandandwales/editions/financial-year-ending-2020/versions/1")
        LOGGER.info("3. Create black_population.csv with columns: msoa,black_count,population")
        LOGGER.info("4. Housing affordability data: housingMSOA140624.xlsx")
        return False
    return True

def _search_shp_member(zf: zipfile.ZipFile) -> str:
    candidates = [m for m in zf.namelist() if m.lower().endswith(".shp") and "msoa" in m.lower() and "london" in m.lower()]
    if not candidates:
        raise RuntimeError("MSOA shapefile not found in boundary ZIP")
    chosen = min(candidates, key=len)
    LOGGER.info("Using shapefile: %s", chosen)
    return chosen

def _identify_msoa_columns(gdf: gpd.GeoDataFrame) -> tuple[str, str]:
    code_col = next((c for c in gdf.columns if c.lower().startswith("msoa") and c.lower().endswith("cd")), None)
    name_col = next((c for c in gdf.columns if c.lower().startswith("msoa") and (
        c.lower().endswith("nm") or "name" in c.lower())), None)
    if not code_col or not name_col:
        raise KeyError("Could not find MSOA code/name columns in shapefile")
    return code_col, name_col

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_income() -> pd.DataFrame:
    """Load income data from local Excel file."""
    xl = pd.ExcelFile(INCOME_XLSX)
    # Use the correct sheet name and skip the first 4 rows to get the header
    df = xl.parse("Total annual income", skiprows=4)
    df = df.rename(columns={
        "MSOA code": "msoa",
        "Total annual income (£)": "income"
    })
    return df[["msoa", "income"]]

def load_black_population() -> pd.DataFrame:
    """Load Black population data from local CSV file."""
    df = pd.read_csv(BLACK_POP_CSV)
    df["black_share"] = df["black_count"] / df["population"]
    return df

def load_housing_affordability() -> pd.DataFrame:
    """Load housing affordability data from local Excel file."""
    xl = pd.ExcelFile(HOUSING_XLSX)
    # Parse the Excel sheet, treating the first row (index 0) as the header
    df = xl.parse("Table 1", header=0)

    # Log the column names to see exactly what pandas identified as headers
    LOGGER.info("Columns identified from housingMSOA140624.xlsx (with header=0): %s", ", ".join(df.columns.tolist()))

    # Rename the columns to our desired standardized names
    # Use the exact strings from the screenshot for the left side of the dict
    df = df.rename(columns={
        "MSOA code": "msoa",
        "MSOA name": "msoa_full_name",
        "All properties": "median_house_price"
    })
    LOGGER.info("Columns after renaming in housingMSOA140624.xlsx: %s", ", ".join(df.columns.tolist()))

    # Convert median_house_price to numeric, coercing errors to NaN
    df["median_house_price"] = pd.to_numeric(df["median_house_price"], errors='coerce')
    # Drop rows where median_house_price is NaN, as these cannot be used in calculations
    original_rows = len(df)
    df.dropna(subset=["median_house_price"], inplace=True)
    if len(df) < original_rows:
        LOGGER.warning("Removed %d rows with non-numeric housing price data.", original_rows - len(df))
    LOGGER.info("After numeric conversion and dropna, 'median_house_price' dtype: %s", df["median_house_price"].dtype)

    # Select and return only the relevant columns
    return df[["msoa", "msoa_full_name", "median_house_price"]]

def load_london_shapes() -> gpd.GeoDataFrame:
    """Load London MSOA boundaries from local ZIP file."""
    with zipfile.ZipFile(BOUNDARIES_ZIP) as zf:
        shp = _search_shp_member(zf)
        with tempfile.TemporaryDirectory() as tmp:
            zf.extractall(tmp)
            gdf = gpd.read_file(Path(tmp) / shp)
    code_col, name_col = _identify_msoa_columns(gdf)
    return gdf[[code_col, name_col, "geometry"]].rename(columns={code_col: "msoa", name_col: "msoa_name"})

# ---------------------------------------------------------------------------
# Analysis & visualisation
# ---------------------------------------------------------------------------

def classify_middle_class(df: pd.DataFrame, low: int = 40, high: int = 60) -> pd.Series:
    """Identify middle-class areas based on income percentiles."""
    lo, hi = df["income"].quantile(low/100), df["income"].quantile(high/100)
    return df["income"].between(lo, hi)

def make_map(gdf: gpd.GeoDataFrame, black_share_col: str, income_col: str, housing_col: str, black_rich_col: str, black_rich_affordable_col: str) -> Map:
    """Create an interactive map with Black population, income, housing affordability, and combined layers."""
    m = Map(location=[51.5074, -0.1278], zoom_start=10, tiles="cartodbpositron")

    # Layer 1 – % Black
    Choropleth(
        geo_data=gdf,
        data=gdf,
        columns=["msoa", black_share_col],
        key_on="feature.properties.msoa",
        name="% Black",
        fill_color="YlOrRd",
        fill_opacity=0.8,
        line_opacity=0.2,
        legend_name="Share identifying as Black (2021)"
    ).add_to(m)

    # Layer 2 – Income
    Choropleth(
        geo_data=gdf,
        data=gdf,
        columns=["msoa", income_col],
        key_on="feature.properties.msoa",
        name="Mean income (£)",
        fill_color="BuGn",
        fill_opacity=0.5,
        line_opacity=0.2,
        legend_name="Mean equivalised disposable income (£, FYE 2020)",
        overlay=True
    ).add_to(m)

    # Layer 3 – Housing Affordability (Median House Price)
    Choropleth(
        geo_data=gdf,
        data=gdf,
        columns=["msoa", housing_col],
        key_on="feature.properties.msoa",
        name="Median House Price (£)",
        fill_color="YlGnBu", # Green-Blue color scale for prices
        fill_opacity=0.6,
        line_opacity=0.2,
        legend_name="Median House Price (All properties, FYE 2023)",
        overlay=True
    ).add_to(m)

    # Layer 4 – Black share >= 15% and income >= mean (original combined layer)
    Choropleth(
        geo_data=gdf,
        data=gdf,
        columns=["msoa", black_rich_col],
        key_on="feature.properties.msoa",
        name="Black ≥15% & Income ≥ mean",
        fill_color="PuRd",
        fill_opacity=0.8,
        line_opacity=0.2,
        legend_name="Black share ≥15% & Income ≥ mean"
    ).add_to(m)

    # Layer 5 – Black share >= 15%, income >= mean, AND affordable housing (<= 65th percentile)
    Choropleth(
        geo_data=gdf,
        data=gdf,
        columns=["msoa", black_rich_affordable_col],
        key_on="feature.properties.msoa",
        name="Black ≥15% & Income ≥ mean & Affordable Housing (65th percentile)",
        fill_color="YlGn", # Using a distinct color scale
        fill_opacity=0.8,
        line_opacity=0.2,
        legend_name="Black share ≥15% & Income ≥ mean & Affordable Housing (65th percentile)"
    ).add_to(m)

    # Add labeled names for Black & Rich & Affordable areas (this will be the dynamic layer)
    # This specific layer will be recreated dynamically in Streamlit based on user input
    # No need to add it here, it will be added in streamlit_app.py

    LayerControl().add_to(m)
    return m

# Removed main() function and __name__ == "__main__" block
# Data loading functions now directly available for import
# ---------------------------------------------------------------------------
# Data loaders are above, no main() block below
# ---------------------------------------------------------------------------
