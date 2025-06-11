import streamlit as st
from streamlit_folium import st_folium
import folium
from folium import FeatureGroup, Marker, DivIcon, LayerControl, Choropleth, Map, GeoJson, GeoJsonPopup, GeoJsonTooltip
import pandas as pd
import geopandas as gpd
import json

# Import functions from your refactored module
from london_black_income_map import (
    check_data_files,
    load_london_shapes,
    load_income,
    load_black_population,
    load_housing_affordability
)

# Initialize a logger for Streamlit app
import logging
LOGGER_ST = logging.getLogger("streamlit-app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S", force=True)


# --- Streamlit Page Setup ---
st.set_page_config(layout="wide", page_title="London Black Middle Class Map")

st.title("London Black Middle Class Map")
st.markdown("Adjust the filters in the sidebar to dynamically highlight MSOAs that meet your criteria.")
st.markdown("The map displays static choropleth layers for general data, and a dynamic layer for filtered areas.")

# --- Data Loading (Cached for performance) ---
@st.cache_data
def load_all_data():
    """Loads and merges all required geospatial and demographic data."""
    if not check_data_files():
        st.error("Missing required data files. Please refer to london_black_income_map.py for download instructions.")
        st.stop() # Stop the app if data is missing

    LOGGER_ST.info("Loading data for Streamlit app...")
    gdf = load_london_shapes()
    income_df = load_income()
    black_df = load_black_population()
    housing_df = load_housing_affordability()

    # Merge data
    LOGGER_ST.info("Merging data...")
    gdf = gdf.merge(income_df, on="msoa", how="left")
    gdf = gdf.merge(black_df, on="msoa", how="left")
    gdf = gdf.merge(housing_df, on="msoa", how="left")
    
    # Drop rows with NaN values in crucial columns after merging
    original_rows = len(gdf)
    gdf.dropna(subset=['black_share', 'income', 'median_house_price'], inplace=True)
    if len(gdf) < original_rows:
        LOGGER_ST.warning(f"Removed {original_rows - len(gdf)} MSOAs due to missing data after merge.")

    return gdf

gdf = load_all_data()

# Calculate global statistics (cached for performance)
@st.cache_data
def calculate_global_stats(income_series: pd.Series, housing_series: pd.Series):
    mean_income = income_series.mean()
    percentile_65_house_price = housing_series.quantile(0.65)
    return mean_income, percentile_65_house_price

mean_income, percentile_65_house_price = calculate_global_stats(gdf["income"], gdf["median_house_price"])

# --- Sidebar Filters ---
st.sidebar.header("Dynamic Filter Criteria")
st.sidebar.write("Adjust these values to update the \"Live Filtered Areas\" layer.")

black_share_threshold = st.sidebar.slider(
    "Minimum Black Population Share (%)",
    min_value=0.0,
    max_value=100.0,
    value=15.0,
    step=1.0,
    format="%.0f%%"
) / 100.0 # Convert to fraction

income_threshold_type = st.sidebar.radio(
    "Income Threshold Type",
    ("Above Mean", "Custom Value"),
    index=0
)
if income_threshold_type == "Above Mean":
    income_threshold_value = mean_income
    st.sidebar.info(f"Mean Income: £{mean_income:,.0f}")
else:
    income_threshold_value = st.sidebar.number_input(
        "Minimum Income (£)",
        min_value=0,
        max_value=int(gdf["income"].max()),
        value=int(mean_income),
        step=1000
    )

housing_threshold_type = st.sidebar.radio(
    "Housing Affordability Threshold Type",
    ("Below 65th Percentile", "Custom Value"),
    index=0
)
if housing_threshold_type == "Below 65th Percentile":
    housing_threshold_value = percentile_65_house_price
    st.sidebar.info(f"65th Percentile House Price: £{percentile_65_house_price:,.0f}")
else:
    housing_threshold_value = st.sidebar.number_input(
        "Maximum House Price (£)",
        min_value=0,
        max_value=int(gdf["median_house_price"].max()),
        value=int(percentile_65_house_price),
        step=1000
    )

# --- Apply Dynamic Filter ---
# Create a boolean mask for the current filter criteria
current_filter_mask = (
    (gdf["black_share"] >= black_share_threshold)
    & (gdf["income"] >= income_threshold_value)
    & (gdf["median_house_price"] <= housing_threshold_value)
)

st.subheader(f"Found {current_filter_mask.sum()} MSOAs matching current criteria")

# --- Sidebar Layer Toggles ---
st.sidebar.header("Map Layer Visibility")
show_black_share = st.sidebar.checkbox("Show % Black (Static)", value=True)
show_income = st.sidebar.checkbox("Show Mean Income (£) (Static)", value=False)
show_house_price = st.sidebar.checkbox("Show Median House Price (£) (Static)", value=False)
show_filtered_areas = st.sidebar.checkbox("Show Live Filtered Areas", value=True)

# --- Create Base Folium Map ---
def create_base_map(dataframe, current_filter_mask_series, show_black_share, show_income, show_house_price, show_filtered_areas):
    m = Map(location=[51.5074, -0.1278], zoom_start=11, tiles="cartodbpositron")

    # Helper function to create custom legend HTML
    def create_custom_legend_html(title, color_stops, bottom_offset):
        legend_html = f"""
        <div style="
            position: absolute;
            bottom: {bottom_offset}px;
            left: 20px; /* Position from left edge */
            z-index: 9999; /* Ensure it's on top of other elements */
            background-color: white; /* Changed to white */
            padding: 12px; /* Smaller padding */
            border-radius: 5px;
            box-shadow: 0 0 5px rgba(0,0,0,0.3); /* Softer shadow */
            font-family: Arial, sans-serif;
            font-size: 12px; /* Smaller font */
            border: 1px solid #ccc; /* Lighter border */
        ">
            <h4 style="margin-top: 0; margin-bottom: 5px; font-size: 14px;">{title}</h4>
        """
        for color, label in color_stops:
            legend_html += f"""
            <div style="display: flex; align-items: center; margin-bottom: 3px;">
                <div style="width: 20px; height: 15px; background-color: {color}; border: 1px solid #ccc; margin-right: 8px;"></div>
                <span>{label}</span>
            </div>
            """
        legend_html += """
        </div>
        """
        return legend_html

    # HELPER FUNCTION: to remove default Folium Choropleth legends
    def remove_folium_choropleth_legend(choropleth_obj):
        del_list = []
        for child in choropleth_obj._children:
            if child.startswith('color_map'):
                del_list.append(child)
        for del_item in del_list:
            choropleth_obj._children.pop(del_item)


    # Layer 1 – % Black (Static Choropleth)
    black_share_choropleth = Choropleth( # Assign to a variable
        geo_data=dataframe,
        data=dataframe,
        columns=["msoa", "black_share"],
        key_on="feature.properties.msoa",
        name="% Black (Static)",
        fill_color="YlOrRd",
        fill_opacity=0.8,
        line_opacity=0.2,
        show=show_black_share # Controlled by sidebar checkbox
    ).add_to(m)
    remove_folium_choropleth_legend(black_share_choropleth) # CALL THE HELPER FUNCTION HERE

    # Layer 2 – Income (Static Choropleth)
    income_choropleth = Choropleth( # Assign to a variable
        geo_data=dataframe,
        data=dataframe,
        columns=["msoa", "income"],
        key_on="feature.properties.msoa",
        name="Mean Income (£) (Static)",
        fill_color="BuGn",
        fill_opacity=0.5,
        line_opacity=0.2,
        overlay=True,
        show=show_income # Controlled by sidebar checkbox
    ).add_to(m)
    remove_folium_choropleth_legend(income_choropleth)

    # Layer 3 – Housing Affordability (Median House Price) (Static Choropleth)
    house_price_choropleth = Choropleth( # Assign to a variable
        geo_data=dataframe,
        data=dataframe,
        columns=["msoa", "median_house_price"],
        key_on="feature.properties.msoa",
        name="Median House Price (£) (Static)",
        fill_color="YlGnBu",
        fill_opacity=0.6,
        line_opacity=0.2,
        overlay=True,
        show=show_house_price # Controlled by sidebar checkbox
    ).add_to(m)
    remove_folium_choropleth_legend(house_price_choropleth)

    # --- Dynamic Layer: Filtered Areas (GeoJson with conditional styling) ---
    # Create a new GeoDataFrame that includes the boolean mask as a property
    gdf_with_filter_mask = dataframe.copy()
    gdf_with_filter_mask["is_filtered"] = current_filter_mask_series.astype(int)

    def style_function(feature):
        # Style based on the 'is_filtered' property
        is_filtered = feature['properties']['is_filtered']
        return {
            'fillColor': '#00CCFF' if is_filtered else 'transparent', # Bright blue for filtered areas
            'color': 'black' if is_filtered else 'transparent',
            'weight': 1 if is_filtered else 0,
            'fillOpacity': 0.7 if is_filtered else 0,
        }

    # Function to create custom HTML for each feature's popup
    def create_feature_popup_html(feature):
        props = feature['properties']
        msoa_name = props['msoa_full_name']
        black_share = props['black_share']
        black_count = props['black_count']
        income = props['income']
        median_house_price = props['median_house_price']

        return f"""
        <b>{msoa_name}</b><br>
        % Black: {black_share:.1%}<br>
        # Black Population: {int(black_count):,}<br>
        Income: £{income:,.0f}<br>
        House Price: £{median_house_price:,.0f}
        """

    # Define tooltip fields (popups are handled by create_feature_popup_html)
    tooltip_fields = ["msoa_full_name", "black_share", "income", "median_house_price"]
    tooltip_aliases = ["MSOA:", "% Black:", "Income (£):", "House Price (£):"]

    GeoJson(
        gdf_with_filter_mask,
        name="Live Filtered Areas",
        style_function=style_function,
        tooltip=GeoJsonTooltip(fields=tooltip_fields, aliases=tooltip_aliases),
        popup=folium.Popup(max_width=300, html=create_feature_popup_html),
        show=show_filtered_areas # Controlled by sidebar checkbox
    ).add_to(m)

    # --- Add Custom Legends (Overlays within Folium Map) ---
    # Approximate colors for YlOrRd (low to high)
    black_share_colors = [
        ("#FFFFCC", "Low (0-10%)"),
        ("#FD8D3C", "Medium (10-30%)"),
        ("#BD0026", "High (30%+) ")
    ]

    # Approximate colors for BuGn (low to high)
    income_colors = [
        ("#EDF8FB", "Low"),
        ("#8FCDAE", "Medium"),
        ("#006D2C", "High")
    ]

    # Approximate colors for YlGnBu (low to high)
    house_price_colors = [
        ("#FFFFD9", "Low"),
        ("#99D594", "Medium"),
        ("#2C7FB8", "High")
    ]

    # Add legends to the map, stacked from bottom
    m.get_root().html.add_child(folium.Element(
        create_custom_legend_html("Median House Price", house_price_colors, 10)
    ))
    m.get_root().html.add_child(folium.Element(
        create_custom_legend_html("Mean Income", income_colors, 130)
    )) # Adjust offset to stack above house price
    m.get_root().html.add_child(folium.Element(
        create_custom_legend_html("% Black Population", black_share_colors, 250)
    )) # Adjust offset to stack above income

    return m

# Create the map using the function
m = create_base_map(gdf.copy(), current_filter_mask, show_black_share, show_income, show_house_price, show_filtered_areas)

# --- Display Map in Streamlit ---
st_folium(m, width=1200, height=700, key="london_map") 