"""Step 2: Geocoding — Convert addresses to lat/lon via OpenStreetMap Nominatim."""

import sys
import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# US state abbreviation lookup — used when no state column is present
_US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}

# All known state abbreviations (both directions)
_STATE_ABBRS = set(_US_STATES.values()) | set(s.upper() for s in _US_STATES)


def _build_fallback_queries(address):
    """
    Generate a list of address strings to try in order.
    Handles cases where state or country info is missing.
    """
    queries = [address]  # Original first

    parts = [p.strip() for p in address.split(",")]

    # Check if any part looks like a US state already
    has_state = any(
        p.upper() in _STATE_ABBRS or p.lower() in _US_STATES for p in parts
    )
    has_usa = any(p.upper() in ("USA", "US", "UNITED STATES") for p in parts)

    if not has_usa:
        # Try with USA appended
        queries.append(address + ", USA")

    if not has_state and not has_usa and len(parts) >= 2:
        # Try inserting "USA" and hope Nominatim resolves the country
        # Also try street + last part (usually city or zip) + USA
        queries.append(f"{parts[0]}, {parts[-1]}, USA")

    return queries


def _geocode_with_fallback(geocode, address):
    """Try multiple query formats until one returns a result."""
    queries = _build_fallback_queries(address)
    for query in queries:
        try:
            location = geocode(query, addressdetails=True)
            if location:
                return location, query
        except Exception:
            pass
        time.sleep(1)  # extra delay between fallback attempts
    return None, None


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
            location, matched_query = _geocode_with_fallback(geocode, address)
            if location:
                if matched_query != address:
                    print(f"  -> Found via: {matched_query}")
                latitudes.append(location.latitude)
                longitudes.append(location.longitude)
                raw = location.raw.get("address", {})
                cities.append(
                    raw.get("city") or raw.get("town") or raw.get("village", "")
                )
                zip_codes.append(raw.get("postcode", ""))
            else:
                print(f"  -> Not found (tried {len(_build_fallback_queries(address))} formats)")
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
