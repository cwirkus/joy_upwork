"""Step 2: Geocoding — Convert addresses to lat/lon via OpenStreetMap Nominatim."""

import sys
import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


def geocode_addresses(df):
    """Add latitude, longitude, city, and zip_code columns to the DataFrame."""
    geolocator = Nominatim(user_agent="property_geocoder_app")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)

    latitudes = []
    longitudes = []
    cities = []
    zip_codes = []

    total = len(df)
    for i, address in enumerate(df["address"]):
        print(f"Geocoding {i + 1}/{total}: {address}")
        try:
            location = geocode(address, addressdetails=True)
            if location:
                latitudes.append(location.latitude)
                longitudes.append(location.longitude)
                raw = location.raw.get("address", {})
                cities.append(
                    raw.get("city") or raw.get("town") or raw.get("village", "")
                )
                zip_codes.append(raw.get("postcode", ""))
            else:
                print(f"  -> Not found")
                latitudes.append(None)
                longitudes.append(None)
                cities.append("")
                zip_codes.append("")
        except Exception as e:
            print(f"  -> Error: {e}")
            latitudes.append(None)
            longitudes.append(None)
            cities.append("")
            zip_codes.append("")

    df["latitude"] = latitudes
    df["longitude"] = longitudes
    df["city"] = cities
    df["zip_code"] = zip_codes

    found = df["latitude"].notna().sum()
    print(f"\nGeocoded {found}/{total} addresses successfully.")

    return df


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python step2.py <cleaned_data.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = "geocoded_data.csv"

    df = pd.read_csv(input_file)
    df = geocode_addresses(df)
    df.to_csv(output_file, index=False)
    print(f"Geocoded data saved to {output_file}")
