import json
import csv
from pathlib import Path

INPUT_FILE = Path(r"C:\Data Projects\canada-aqhi-pipeline\data\raw\aqhi_station.geojson")
OUTPUT_FILE = Path(r"C:\Data Projects\canada-aqhi-pipeline\data\processed\stations_metadata.csv")

def extract_station_metadata():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        geojson = json.load(f)

    features = geojson['features']

    rows = []
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get("geometry", {})

        station_code = props.get('cgndb')
        station_name = props.get('name', {}).get('en')
        obs_url = props.get("path_to_current_observation")
        fcst_url = props.get("path_to_current_forecast")

        coords = geom.get('coordinates', [None, None])
        longtitude = coords[0], coords[1]

        rows.append({
            "station_code": station_code,
            "station_name": station_name,
            "longitude": longtitude[0],
            "latitude": longtitude[1],
            "observations_url": obs_url,
            "forecast_url": fcst_url
        })

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} stations -> {OUTPUT_FILE}")

if __name__ == "__main__":
    extract_station_metadata()