"""Flask web application — Upload addresses, run the full pipeline, view results."""

import os
import numpy as np
import pandas as pd
import folium
from folium.plugins import HeatMap
from flask import Flask, render_template, request, redirect, url_for, send_file, flash
 
from step1 import load_and_clean
from step2 import geocode_addresses
from step3 import detect_clusters
from step4 import generate_heatmap

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "property-geocoder-dev-secret")

# Use RAILWAY_VOLUME_MOUNT_PATH for persistent storage on Railway,
# otherwise fall back to local directories for development.
DATA_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(DATA_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(DATA_DIR, "output")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

ADDRESSES_CSV = os.path.join(OUTPUT_FOLDER, "saved_addresses.csv")
ALLOWED_EXTENSIONS = {"csv", "xls", "xlsx"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/")
def index():
    # Check if results exist
    enriched_path = os.path.join(OUTPUT_FOLDER, "enriched_data.csv")
    heatmap_path = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    has_results = os.path.exists(enriched_path) and os.path.exists(heatmap_path)
    return render_template("index.html", has_results=has_results)


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        flash("No file selected.")
        return redirect(url_for("index"))

    file = request.files["file"]
    if file.filename == "":
        flash("No file selected.")
        return redirect(url_for("index"))

    if not allowed_file(file.filename):
        flash("Invalid file type. Please upload a CSV or Excel file.")
        return redirect(url_for("index"))

    # Save uploaded file
    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)

    try:
        # Step 1: Clean
        df = load_and_clean(filepath)

        # Steps 2-4: Geocode, cluster, heatmap
        df = _run_pipeline(df)

        flash(f"Pipeline complete! Processed {len(df)} addresses.")
    except Exception as e:
        flash(f"Error during processing: {e}")
        return redirect(url_for("index"))

    return redirect(url_for("index"))


def _run_pipeline(df):
    """Run steps 2-4 on a DataFrame that already has an 'address' column."""
    df = geocode_addresses(df)
    df = detect_clusters(df)

    heatmap_path = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    generate_heatmap(df, heatmap_path)

    enriched_path = os.path.join(OUTPUT_FOLDER, "enriched_data.csv")
    df.to_csv(enriched_path, index=False)
    return df


def _load_saved_addresses():
    """Load the saved addresses CSV, or return an empty DataFrame."""
    if os.path.exists(ADDRESSES_CSV):
        return pd.read_csv(ADDRESSES_CSV)
    return pd.DataFrame(columns=["address"])


def _save_addresses(df):
    """Save addresses DataFrame to the persistent CSV."""
    df.to_csv(ADDRESSES_CSV, index=False)


@app.route("/add-address", methods=["POST"])
def add_address():
    raw = request.form.get("addresses", "").strip()
    if not raw:
        flash("No addresses entered.")
        return redirect(url_for("index"))

    # Split by newlines, clean each line
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        flash("No valid addresses entered.")
        return redirect(url_for("index"))

    new_df = pd.DataFrame({"address": lines})

    # Append to existing saved addresses
    existing = _load_saved_addresses()
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["address"])
    _save_addresses(combined)

    flash(f"Added {len(lines)} address(es). Total: {len(combined)} saved.")
    return redirect(url_for("addresses"))


@app.route("/delete-address", methods=["POST"])
def delete_address():
    address = request.form.get("address", "")
    if not address:
        return redirect(url_for("addresses"))

    df = _load_saved_addresses()
    df = df[df["address"] != address]
    _save_addresses(df)

    flash(f"Removed address.")
    return redirect(url_for("addresses"))


@app.route("/process-saved", methods=["POST"])
def process_saved():
    """Run the full pipeline on all saved addresses."""
    df = _load_saved_addresses()
    if len(df) == 0:
        flash("No saved addresses to process.")
        return redirect(url_for("addresses"))

    try:
        df = _run_pipeline(df)
        # Save enriched data back so addresses list has lat/lon/cluster
        _save_addresses(df)
        flash(f"Pipeline complete! Processed {len(df)} addresses.")
    except Exception as e:
        flash(f"Error during processing: {e}")

    return redirect(url_for("addresses"))


def _get_density_label(cluster_id, df):
    """Return a density label based on how many addresses share the same cluster."""
    if pd.isna(cluster_id) or int(cluster_id) == -1:
        return "Isolated"
    cluster_size = (df["cluster"] == cluster_id).sum()
    if cluster_size >= 10:
        return "High"
    elif cluster_size >= 5:
        return "Medium"
    return "Low"


def _get_density_badge(label):
    """Return CSS class suffix for density badge."""
    return {"High": "danger", "Medium": "warning", "Low": "info", "Isolated": "muted"}.get(label, "muted")


@app.route("/addresses")
def addresses():
    df = _load_saved_addresses()
    enriched_path = os.path.join(OUTPUT_FOLDER, "enriched_data.csv")
    heatmap_path = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    has_results = os.path.exists(enriched_path) and os.path.exists(heatmap_path)

    # Build address list with density info
    address_data = []
    has_geocoded = "latitude" in df.columns and "cluster" in df.columns
    for i, row in df.iterrows():
        entry = {"index": i, "address": row["address"]}
        if has_geocoded and pd.notna(row.get("latitude")):
            label = _get_density_label(row.get("cluster", -1), df)
            entry["density"] = label
            entry["badge"] = _get_density_badge(label)
            entry["geocoded"] = True
        else:
            entry["density"] = "Not processed"
            entry["badge"] = "muted"
            entry["geocoded"] = False
        address_data.append(entry)

    return render_template("addresses.html", address_data=address_data,
                           has_results=has_results, count=len(address_data))


@app.route("/view-address/<int:idx>")
def view_address(idx):
    """Show an individual address on its own map with nearby addresses."""
    df = _load_saved_addresses()
    if idx < 0 or idx >= len(df):
        flash("Address not found.")
        return redirect(url_for("addresses"))

    row = df.iloc[idx]
    address = row["address"]

    has_coords = "latitude" in df.columns and pd.notna(row.get("latitude"))
    if not has_coords:
        flash(f'Address "{address}" has not been geocoded yet. Process addresses first.')
        return redirect(url_for("addresses"))

    lat, lon = row["latitude"], row["longitude"]
    cluster_id = row.get("cluster", -1)

    # Build the map centered on this address
    m = folium.Map(location=[lat, lon], zoom_start=15)

    # Add marker for the selected address
    folium.Marker(
        [lat, lon],
        popup=f"<b>{address}</b>",
        tooltip=address,
        icon=folium.Icon(color="red", icon="home", prefix="fa"),
    ).add_to(m)

    # Add nearby addresses from the same cluster
    nearby = []
    if "cluster" in df.columns and not pd.isna(cluster_id) and int(cluster_id) != -1:
        cluster_mates = df[(df["cluster"] == cluster_id) & (df.index != idx)]
        for _, mate in cluster_mates.iterrows():
            if pd.notna(mate.get("latitude")):
                folium.Marker(
                    [mate["latitude"], mate["longitude"]],
                    popup=mate["address"],
                    tooltip=mate["address"],
                    icon=folium.Icon(color="blue", icon="building", prefix="fa"),
                ).add_to(mate_marker_group := folium.FeatureGroup(name="Nearby"))
                nearby.append(mate["address"])
        if nearby:
            mate_marker_group.add_to(m)

    # Add a small heatmap around the area using all geocoded addresses
    all_valid = df.dropna(subset=["latitude", "longitude"])
    if len(all_valid) > 1:
        heat_data = all_valid[["latitude", "longitude"]].values.tolist()
        HeatMap(heat_data, radius=15, blur=10, max_zoom=13).add_to(m)

    # Save per-address map
    map_path = os.path.join(OUTPUT_FOLDER, f"address_{idx}.html")
    m.save(map_path)

    density = _get_density_label(cluster_id, df) if "cluster" in df.columns else "Unknown"
    cluster_size = int((df["cluster"] == cluster_id).sum()) if "cluster" in df.columns and not pd.isna(cluster_id) and int(cluster_id) != -1 else 0

    return render_template("view_address.html",
                           address=address, idx=idx,
                           lat=lat, lon=lon,
                           density=density,
                           badge=_get_density_badge(density),
                           cluster_size=cluster_size,
                           nearby=nearby,
                           city=row.get("city", ""),
                           zip_code=row.get("zip_code", ""))


@app.route("/address-map/<int:idx>")
def address_map(idx):
    """Serve the generated per-address map HTML."""
    map_path = os.path.join(OUTPUT_FOLDER, f"address_{idx}.html")
    if os.path.exists(map_path):
        return send_file(map_path)
    return "Map not available", 404


@app.route("/heatmap")
def heatmap():
    heatmap_path = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    if not os.path.exists(heatmap_path):
        flash("No heatmap available. Upload data first.")
        return redirect(url_for("index"))
    return render_template("heatmap.html")


@app.route("/heatmap-raw")
def heatmap_raw():
    heatmap_path = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    if os.path.exists(heatmap_path):
        return send_file(heatmap_path)
    return "No heatmap available", 404


@app.route("/download")
def download():
    enriched_path = os.path.join(OUTPUT_FOLDER, "enriched_data.csv")
    if os.path.exists(enriched_path):
        return send_file(enriched_path, as_attachment=True)
    flash("No data available. Upload data first.")
    return redirect(url_for("index"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    app.run(debug=debug, host="0.0.0.0", port=port)
