"""
Script to download and process Census 2021 data for Black population in London MSOAs.

This script will:
1. Download the Census 2021 dataset TS021 (Ethnic group)
2. Process it to get Black population counts for London MSOAs
3. Save the results as black_population.csv

Run:
```bash
python download_census_data.py
```
"""

import logging
import requests
import pandas as pd
from pathlib import Path
import time
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
LOGGER = logging.getLogger("census-downloader")

# Constants
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "black_population.csv"

# Census API parameters
CENSUS_DATASET_ID = "TS021"
CENSUS_VERSION = 1
DEFAULT_TIMEOUT = 60
MAX_RETRIES = 3

# API Headers
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "London-Black-Income-Map/1.0"
}

def safe_json(resp: requests.Response) -> dict:
    """Safely parse JSON response."""
    try:
        return resp.json()
    except ValueError as exc:
        LOGGER.error("Bad JSON (%d). Body: %s", resp.status_code, resp.text[:400])
        raise RuntimeError("ONS API returned non-JSON") from exc

def get_dimensions() -> dict:
    """Get available dimensions from the dataset."""
    url = f"https://api.beta.ons.gov.uk/v1/datasets/{CENSUS_DATASET_ID}/editions/2021/versions/{CENSUS_VERSION}/dimensions"
    
    for attempt in range(MAX_RETRIES):
        try:
            LOGGER.info("Fetching dataset dimensions...")
            r = requests.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
            return safe_json(r)
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"Failed to fetch dimensions after {MAX_RETRIES} attempts: {exc}")
            sleep = 2 ** attempt
            LOGGER.warning("Request failed, retrying in %d seconds...", sleep)
            time.sleep(sleep)

def fetch_census_data() -> list:
    """Fetch data from Census API for all ethnic groups."""
    base_url = f"https://api.beta.ons.gov.uk/v1/datasets/{CENSUS_DATASET_ID}/editions/2021/versions/{CENSUS_VERSION}/observations"
    
    # Initial request parameters
    params = {
        "area_type": "msoa",
        "dimensions": "ethnic_group,sex"
    }
    
    all_observations = []
    url = base_url
    
    while url:
        for attempt in range(MAX_RETRIES):
            try:
                LOGGER.info("Fetching census data...")
                r = requests.get(url, params=params if url == base_url else None, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
                r.raise_for_status()
                data = safe_json(r)
                all_observations.extend(data["observations"])
                url = data.get("pagination", {}).get("next")
                break
            except requests.exceptions.RequestException as exc:
                if attempt == MAX_RETRIES - 1:
                    raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts: {exc}")
                sleep = 2 ** attempt
                LOGGER.warning("Request failed, retrying in %d seconds...", sleep)
                time.sleep(sleep)
    
    return all_observations

def process_observations(observations: list) -> pd.DataFrame:
    """Process observations into a DataFrame."""
    data = []
    
    for obs in observations:
        dims = obs["dimensions"]
        if dims["sex"]["label"] != "All persons":
            continue
            
        data.append({
            "msoa": dims["geography"]["id"],
            "ethnic_group": dims["ethnic_group"]["label"],
            "count": int(obs["observation"])
        })
    
    df = pd.DataFrame(data)
    
    # Calculate total population
    total = df[df["ethnic_group"] == "All ethnic groups"].copy()
    total = total.rename(columns={"count": "population"})
    total = total[["msoa", "population"]]
    
    # Calculate Black population
    black = df[df["ethnic_group"] == "Black, Black British, Black Welsh, Caribbean or African"].copy()
    black = black.rename(columns={"count": "black_count"})
    black = black[["msoa", "black_count"]]
    
    # Merge and return
    return black.merge(total, on="msoa")

def main():
    """Main function to download and process Census data."""
    try:
        # First, get dimensions to verify API access
        dimensions = get_dimensions()
        LOGGER.info("Successfully connected to ONS API")
        
        # Fetch all data
        LOGGER.info("Fetching census data...")
        observations = fetch_census_data()
        
        # Process data
        LOGGER.info("Processing data...")
        result_df = process_observations(observations)
        
        # Save results
        LOGGER.info("Saving data to %s", OUTPUT_FILE)
        result_df.to_csv(OUTPUT_FILE, index=False)
        
        LOGGER.info("Found %d MSOAs with Black population data", len(result_df))
        
    except Exception as e:
        LOGGER.error("Failed to download Census data: %s", e)
        raise

if __name__ == "__main__":
    main() 