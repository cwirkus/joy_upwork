"""Step 3: Cluster Detection — Find high-activity areas using DBSCAN."""

import sys
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN


def detect_clusters(df, eps_km=0.5, min_samples=3):
    """Apply DBSCAN clustering on geocoded coordinates.

    Args:
        df: DataFrame with 'latitude' and 'longitude' columns.
        eps_km: Maximum distance (km) between points in a cluster.
        min_samples: Minimum points to form a cluster.

    Returns:
        DataFrame with added 'cluster' column.
    """
    # Filter to rows with valid coordinates
    has_coords = df["latitude"].notna() & df["longitude"].notna()
    valid_df = df[has_coords].copy()

    if len(valid_df) == 0:
        print("No valid coordinates to cluster.")
        df["cluster"] = -1
        return df

    # Convert to radians for haversine metric
    coords_rad = np.radians(valid_df[["latitude", "longitude"]].values)

    # eps in radians = eps_km / Earth's radius (6371 km)
    eps_rad = eps_km / 6371.0

    db = DBSCAN(eps=eps_rad, min_samples=min_samples, metric="haversine")
    labels = db.fit_predict(coords_rad)

    valid_df["cluster"] = labels
    df["cluster"] = -1
    df.loc[has_coords, "cluster"] = valid_df["cluster"].values

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = (labels == -1).sum()
    print(f"Found {n_clusters} clusters, {n_noise} noise points out of {len(valid_df)} total.")

    return df


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python step3.py <geocoded_data.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = "clustered_data.csv"

    df = pd.read_csv(input_file)
    df = detect_clusters(df)
    df.to_csv(output_file, index=False)
    print(f"Clustered data saved to {output_file}")
