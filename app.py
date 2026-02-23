"""Flask web application — Upload addresses, run the full pipeline, view results."""

import os
import json
import threading
import numpy as np
import pandas as pd
import folium
from folium.plugins import HeatMap
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify

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
STATUS_FILE   = os.path.join(OUTPUT_FOLDER, "job_status.json")
ALLOWED_EXTENSIONS = {"csv", "xls", "xlsx"}

# Lock so only one pipeline job runs at a time
_pipeline_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

def _write_status(state, message, total=0, done=0):
    payload = {"state": state, "message": message, "total": total, "done": done}
    with open(STATUS_FILE, "w") as f:
        json.dump(payload, f)


def _read_status():
    if not os.path.exists(STATUS_FILE):
        return {"state": "idle", "message": "", "total": 0, "done": 0}
    with open(STATUS_FILE) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _load_saved_addresses():
    if os.path.exists(ADDRESSES_CSV):
        return pd.read_csv(ADDRESSES_CSV)
    return pd.DataFrame(columns=["address"])


def _save_addresses(df):
    df.to_csv(ADDRESSES_CSV, index=False)


def _get_density_label(cluster_id, df):
    if pd.isna(cluster_id) or int(cluster_id) == -1:
        return "Isolated"
    cluster_size = (df["cluster"] == cluster_id).sum()
    if cluster_size >= 10:
        return "High"
    elif cluster_size >= 5:
        return "Medium"
    return "Low"


def _get_density_badge(label):
    return {"High": "danger", "Medium": "warning", "Low": "info", "Isolated": "muted"}.get(label, "muted")


# ---------------------------------------------------------------------------
# Background pipeline
# ---------------------------------------------------------------------------

def _run_pipeline_background(df, save_to_addresses=False):
    """Run the full geocode → cluster → heatmap pipeline in a background thread."""
    try:
        total = len(df)
        _write_status("running", f"Geocoding addresses… (0/{total})", total=total, done=0)

        # Geocode with live progress updates
        from geopy.geocoders import Nominatim
        from geopy.extra.rate_limiter import RateLimiter
        import time

        geolocator = Nominatim(user_agent="property_geocoder_app")
        geocode_fn = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)

        latitudes, longitudes, cities, zip_codes = [], [], [], []

        for i, address in enumerate(df["address"]):
            _write_status("running", f"Geocoding {i + 1}/{total}: {address}",
                          total=total, done=i)
            try:
                from step2 import _build_fallback_queries
                import time as _time
                location = None
                for query in _build_fallback_queries(address):
                    try:
                        location = geocode_fn(query, addressdetails=True)
                        if location:
                            break
                    except Exception:
                        pass
                    _time.sleep(1)

                if location:
                    latitudes.append(location.latitude)
                    longitudes.append(location.longitude)
                    raw = location.raw.get("address", {})
                    cities.append(raw.get("city") or raw.get("town") or raw.get("village", ""))
                    zip_codes.append(raw.get("postcode", ""))
                else:
                    latitudes.append(None)
                    longitudes.append(None)
                    cities.append("")
                    zip_codes.append("")
            except Exception:
                latitudes.append(None)
                longitudes.append(None)
                cities.append("")
                zip_codes.append("")

        df["latitude"]  = latitudes
        df["longitude"] = longitudes
        df["city"]      = cities
        df["zip_code"]  = zip_codes

        found = df["latitude"].notna().sum()
        _write_status("running", f"Geocoded {found}/{total}. Running cluster analysis…",
                      total=total, done=total)

        df = detect_clusters(df)

        _write_status("running", "Generating heatmap…", total=total, done=total)

        heatmap_path = os.path.join(OUTPUT_FOLDER, "heatmap.html")
        generate_heatmap(df, heatmap_path)

        enriched_path = os.path.join(OUTPUT_FOLDER, "enriched_data.csv")
        df.to_csv(enriched_path, index=False)

        if save_to_addresses:
            _save_addresses(df)

        _write_status("complete",
                      f"Done! Geocoded {found}/{total} addresses successfully.",
                      total=total, done=total)

    except Exception as e:
        _write_status("error", f"Error: {e}", total=0, done=0)


def _start_pipeline(df, save_to_addresses=False):
    """Acquire lock and launch pipeline in a background thread."""
    if not _pipeline_lock.acquire(blocking=False):
        return False  # already running
    def _run():
        try:
            _run_pipeline_background(df, save_to_addresses=save_to_addresses)
        finally:
            _pipeline_lock.release()
    threading.Thread(target=_run, daemon=True).start()
    return True


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    enriched_path = os.path.join(OUTPUT_FOLDER, "enriched_data.csv")
    heatmap_path  = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    has_results   = os.path.exists(enriched_path) and os.path.exists(heatmap_path)
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

    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    try:
        file.save(filepath)
        df = load_and_clean(filepath)
    except Exception as e:
        flash(f"Could not read file: {e}")
        return redirect(url_for("index"))

    if not _start_pipeline(df, save_to_addresses=True):
        flash("A job is already running. Please wait for it to finish.")
        return redirect(url_for("processing"))

    return redirect(url_for("processing"))


@app.route("/add-address", methods=["POST"])
def add_address():
    raw = request.form.get("addresses", "").strip()
    if not raw:
        flash("No addresses entered.")
        return redirect(url_for("index"))

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        flash("No valid addresses entered.")
        return redirect(url_for("index"))

    new_df   = pd.DataFrame({"address": lines})
    existing = _load_saved_addresses()
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["address"])
    _save_addresses(combined)

    flash(f"Added {len(lines)} address(es). Total: {len(combined)} saved.")
    return redirect(url_for("addresses"))


@app.route("/delete-address", methods=["POST"])
def delete_address():
    address = request.form.get("address", "")
    if address:
        df = _load_saved_addresses()
        df = df[df["address"] != address]
        _save_addresses(df)
        flash("Removed address.")
    return redirect(url_for("addresses"))


@app.route("/process-saved", methods=["POST"])
def process_saved():
    df = _load_saved_addresses()
    if len(df) == 0:
        flash("No saved addresses to process.")
        return redirect(url_for("addresses"))

    if not _start_pipeline(df, save_to_addresses=True):
        flash("A job is already running. Please wait for it to finish.")
        return redirect(url_for("processing"))

    return redirect(url_for("processing"))


@app.route("/processing")
def processing():
    return render_template("processing.html")


@app.route("/status")
def status():
    return jsonify(_read_status())


@app.route("/addresses")
def addresses():
    df = _load_saved_addresses()
    enriched_path = os.path.join(OUTPUT_FOLDER, "enriched_data.csv")
    heatmap_path  = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    has_results   = os.path.exists(enriched_path) and os.path.exists(heatmap_path)

    address_data  = []
    has_geocoded  = "latitude" in df.columns and "cluster" in df.columns
    for i, row in df.iterrows():
        entry = {"index": i, "address": row["address"]}
        if has_geocoded and pd.notna(row.get("latitude")):
            label = _get_density_label(row.get("cluster", -1), df)
            entry.update(density=label, badge=_get_density_badge(label), geocoded=True)
        else:
            entry.update(density="Not processed", badge="muted", geocoded=False)
        address_data.append(entry)

    return render_template("addresses.html", address_data=address_data,
                           has_results=has_results, count=len(address_data))


@app.route("/view-address/<int:idx>")
def view_address(idx):
    df = _load_saved_addresses()
    if idx < 0 or idx >= len(df):
        flash("Address not found.")
        return redirect(url_for("addresses"))

    row     = df.iloc[idx]
    address = row["address"]

    if "latitude" not in df.columns or pd.isna(row.get("latitude")):
        flash(f'"{address}" has not been geocoded yet. Process addresses first.')
        return redirect(url_for("addresses"))

    lat, lon   = row["latitude"], row["longitude"]
    cluster_id = row.get("cluster", -1)

    m = folium.Map(location=[lat, lon], zoom_start=15)
    folium.Marker([lat, lon], popup=f"<b>{address}</b>", tooltip=address,
                  icon=folium.Icon(color="red", icon="home", prefix="fa")).add_to(m)

    nearby = []
    if "cluster" in df.columns and not pd.isna(cluster_id) and int(cluster_id) != -1:
        cluster_mates = df[(df["cluster"] == cluster_id) & (df.index != idx)]
        grp = folium.FeatureGroup(name="Nearby")
        for _, mate in cluster_mates.iterrows():
            if pd.notna(mate.get("latitude")):
                folium.Marker([mate["latitude"], mate["longitude"]],
                              popup=mate["address"], tooltip=mate["address"],
                              icon=folium.Icon(color="blue", icon="building", prefix="fa")).add_to(grp)
                nearby.append(mate["address"])
        if nearby:
            grp.add_to(m)

    all_valid = df.dropna(subset=["latitude", "longitude"])
    if len(all_valid) > 1:
        HeatMap(all_valid[["latitude", "longitude"]].values.tolist(),
                radius=15, blur=10, max_zoom=13).add_to(m)

    map_path = os.path.join(OUTPUT_FOLDER, f"address_{idx}.html")
    m.save(map_path)

    density     = _get_density_label(cluster_id, df) if "cluster" in df.columns else "Unknown"
    cluster_size = int((df["cluster"] == cluster_id).sum()) if (
        "cluster" in df.columns and not pd.isna(cluster_id) and int(cluster_id) != -1
    ) else 0

    return render_template("view_address.html", address=address, idx=idx,
                           lat=lat, lon=lon, density=density,
                           badge=_get_density_badge(density),
                           cluster_size=cluster_size, nearby=nearby,
                           city=row.get("city", ""), zip_code=row.get("zip_code", ""))


@app.route("/address-map/<int:idx>")
def address_map(idx):
    map_path = os.path.join(OUTPUT_FOLDER, f"address_{idx}.html")
    if os.path.exists(map_path):
        return send_file(map_path)
    return "Map not available", 404


@app.route("/heatmap")
def heatmap():
    heatmap_path = os.path.join(OUTPUT_FOLDER, "heatmap.html")
    if not os.path.exists(heatmap_path):
        flash("No heatmap available yet.")
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
    port  = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    app.run(debug=debug, host="0.0.0.0", port=port)
