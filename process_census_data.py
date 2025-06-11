"""
Script to process local Census 2021 data for Black population in London MSOAs.

This script will:
1. Read the Census 2021 dataset TS021 CSV file
2. Process it to get Black population counts for London MSOAs
3. Save the results as black_population.csv

Run:
```bash
python process_census_data.py
```
"""

import logging
import pandas as pd
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
LOGGER = logging.getLogger("census-processor")

# Constants
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
INPUT_FILE = DATA_DIR / "census_ts021.csv"
OUTPUT_FILE = DATA_DIR / "black_population.csv"

def process_census_data() -> pd.DataFrame:
    """Process the Census CSV file to extract Black population data (all subcategories)."""
    LOGGER.info("Reading Census data from %s", INPUT_FILE)
    
    # Read the CSV file
    df = pd.read_csv(INPUT_FILE)
    
    geo_col = "Middle layer Super Output Areas Code"
    ethnic_col = "Ethnic group (20 categories)"
    value_col = "Observation"
    
    # Filter for MSOA level data (codes start with E02)
    msoa_data = df[df[geo_col].astype(str).str.startswith("E02")].copy()
    
    # Calculate total population as the sum of all ethnic group counts per MSOA
    total = msoa_data.groupby(geo_col)[value_col].sum().reset_index()
    total = total.rename(columns={value_col: "population"})
    
    # Get all Black subcategories (ethnic group starts with the prefix)
    black_mask = msoa_data[ethnic_col].str.startswith(
        "Black, Black British, Black Welsh, Caribbean or African:"
    )
    black = msoa_data[black_mask].copy()
    # Sum all Black subcategories per MSOA
    black_sum = black.groupby(geo_col)[value_col].sum().reset_index()
    black_sum = black_sum.rename(columns={value_col: "black_count"})
    
    # Merge and rename columns
    result = black_sum.merge(total, on=geo_col)
    result = result.rename(columns={geo_col: "msoa"})
    
    return result

def main():
    """Main function to process Census data."""
    try:
        if not INPUT_FILE.exists():
            LOGGER.error("Census data file not found: %s", INPUT_FILE)
            LOGGER.info("Please download the Census 2021 TS021 dataset from:")
            LOGGER.info("https://www.ons.gov.uk/datasets/TS021/editions/2021/versions/1")
            LOGGER.info("Save it as %s", INPUT_FILE)
            return
        
        # Process the data
        result_df = process_census_data()
        
        # Save results
        LOGGER.info("Saving data to %s", OUTPUT_FILE)
        result_df.to_csv(OUTPUT_FILE, index=False)
        
        LOGGER.info("Found %d MSOAs with Black population data", len(result_df))
        
    except Exception as e:
        LOGGER.error("Failed to process Census data: %s", e)
        raise

if __name__ == "__main__":
    main() 