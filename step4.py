"""Step 4: Heatmap Generation — Create an interactive HTML heatmap with Folium."""

import sys
import pandas as pd
import folium
from folium.plugins import HeatMap


def generate_heatmap(df, output_file="heatmap.html"):
    """Generate an interactive heatmap from geocoded coordinates.

    Args:
        df: DataFrame with 'latitude' and 'longitude' columns.
        output_file: Path for the output HTML file.

    Returns:
        Path to the generated HTML file.
    """
    # Filter to valid coordinates
    valid = df.dropna(subset=["latitude", "longitude"])

    if len(valid) == 0:
        print("No valid coordinates for heatmap.")
        return None

    # Center the map on the mean location
    center_lat = valid["latitude"].mean()
    center_lon = valid["longitude"].mean()

    m = folium.Map(location=[center_lat, center_lon], zoom_start=11)

    # Build heat data
    heat_data = valid[["latitude", "longitude"]].values.tolist()
    HeatMap(heat_data, radius=15, blur=10, max_zoom=13).add_to(m)

    m.save(output_file)
    print(f"Heatmap saved to {output_file} ({len(heat_data)} points)")
    return output_file


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python step4.py <clustered_data.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = "heatmap.html"

    df = pd.read_csv(input_file)
    generate_heatmap(df, output_file)
