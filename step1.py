"""Step 1: Data Intake & Cleaning — Load CSV/Excel and clean property addresses."""

import sys
import pandas as pd


def load_and_clean(file_path):
    """Load a CSV or Excel file and return a cleaned DataFrame."""
    ext = file_path.rsplit(".", 1)[-1].lower()
    if ext in ("xls", "xlsx"):
        df = pd.read_excel(file_path)
    elif ext == "csv":
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: .{ext}")

    # Find the address column (case-insensitive)
    addr_col = None
    for col in df.columns:
        if col.strip().lower() == "address":
            addr_col = col
            break

    if addr_col is None:
        raise ValueError(
            f"No 'address' column found. Columns available: {list(df.columns)}"
        )

    # Rename to standard name
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
