"""Step 1: Data Intake & Cleaning — Load CSV/Excel and clean property addresses."""

import sys
import pandas as pd


# Common column name variations (lowercase) mapped to standard component names
COLUMN_ALIASES = {
    "address": "street",
    "street": "street",
    "street address": "street",
    "street_address": "street",
    "property address": "street",
    "property_address": "street",
    "addr": "street",
    "address1": "street",
    "address 1": "street",
    "city": "city",
    "town": "city",
    "municipality": "city",
    "state": "state",
    "st": "state",
    "province": "state",
    "zip": "zip",
    "zipcode": "zip",
    "zip code": "zip",
    "zip_code": "zip",
    "postal": "zip",
    "postal code": "zip",
    "postal_code": "zip",
}


def _find_columns(df):
    """Auto-detect address-related columns. Returns a dict of component -> column name."""
    found = {}
    col_map = {col.strip().lower(): col for col in df.columns}

    for alias, component in COLUMN_ALIASES.items():
        if alias in col_map and component not in found:
            found[component] = col_map[alias]

    return found


def load_and_clean(file_path):
    """Load a CSV or Excel file and return a cleaned DataFrame with an 'address' column.

    Handles two formats:
      1. A single 'address' column with full addresses
      2. Separate columns for street, city, state, zip — auto-detected and merged
    """
    ext = file_path.rsplit(".", 1)[-1].lower()
    if ext in ("xls", "xlsx"):
        df = pd.read_excel(file_path)
    elif ext == "csv":
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: .{ext}")

    print(f"Detected columns: {list(df.columns)}")

    # Try to detect address component columns
    detected = _find_columns(df)
    print(f"Matched components: {detected}")

    if "street" in detected and len(detected) >= 2:
        # Multiple address component columns found — merge them
        parts = ["street", "city", "state", "zip"]
        available = [detected[p] for p in parts if p in detected]

        print(f"Combining columns: {available} into 'address'")

        # Fill NaN with empty string for merging, then join with ", "
        for col in available:
            df[col] = df[col].astype(str).fillna("").str.strip()

        df["address"] = df[available].apply(
            lambda row: ", ".join(val for val in row if val and val.lower() != "nan"),
            axis=1,
        )

    elif "street" in detected:
        # Only a street column, use it as-is
        df = df.rename(columns={detected["street"]: "address"})

    else:
        # Look for a single combined "address" column as a fallback
        addr_col = None
        for col in df.columns:
            if col.strip().lower() == "address":
                addr_col = col
                break

        if addr_col is None:
            raise ValueError(
                f"Could not auto-detect address columns.\n"
                f"Columns found: {list(df.columns)}\n"
                f"Expected either a single 'Address' column, or separate "
                f"'Address'/'Street', 'City', 'State', 'Zip' columns."
            )
        df = df.rename(columns={addr_col: "address"})

    original_count = len(df)

    # Drop rows with missing addresses
    df = df.dropna(subset=["address"])

    # Clean: strip whitespace, collapse multiple spaces
    df["address"] = df["address"].astype(str).str.strip()
    df["address"] = df["address"].str.replace(r"\s+", " ", regex=True)

    # Drop empty strings
    df = df[df["address"] != ""]

    df = df.reset_index(drop=True)

    removed = original_count - len(df)
    print(f"Loaded {original_count} rows, removed {removed} invalid, {len(df)} remaining.")

    return df


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python step1.py <input_file.csv|xlsx>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = "cleaned_data.csv"

    df = load_and_clean(input_file)
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")
