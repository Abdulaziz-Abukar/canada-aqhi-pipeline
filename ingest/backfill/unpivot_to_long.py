from pathlib import Path
import pandas as pd

# Paths
PROCESSED_DIR = Path(r"C:\Data Projects\canada-aqhi-pipeline\data\processed")
INPUT_FILE = PROCESSED_DIR / "aqhi_pnr_2023_2025_combined.csv"
OUTPUT_FILE = PROCESSED_DIR / "aqhi_pnr_2023_2025_long.csv"

def main():
    print(f"Reading from {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)

    # 1) Create a proper datetime column from Date + Hour (UTC)
    # Assumes Date is like YYYY-MM-DD and Hour (UTC) is 0-23
    print("Creating datetime_utc column...")
    df['datetime_utc'] = pd.to_datetime(df['Date']) + pd.to_timedelta(df['Hour (UTC)'], unit='h')

    # 2) Identify which columns are stations
    # We keep only datetime_utc + source_file as id columns,
    # everything else (except original Date/Hour) is assumed to be a station code.
    id_cols = ['datetime_utc', 'source_file']
    exclude_cols = {"Date", "Hour (UTC)", "source_file", "datetime_utc"}
    stations_cols = [col for col in df.columns if col not in exclude_cols]

    print(f"Found {len(stations_cols)} station columns to unpivot.")

    # 3) Melt from wide to long
    print("Unpivoting data to long format...")
    long_df = df.melt(
        id_vars=id_cols,
        value_vars=stations_cols,
        var_name="station_code",
        value_name="aqhi_value"
    )

    # 4) Drop rows with no AQHI value
    before = len(long_df)
    long_df = long_df.dropna(subset=['aqhi_value'])
    after = len(long_df)
    print(f"Dropped {before - after} rows with missing AQHI values. Remaining rows: {after}.")

    # 5) Cast aqhi_value to float
    long_df['aqhi_value'] = long_df['aqhi_value'].astype(float)

    # 6) Save to new CSV
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing long-format data to {OUTPUT_FILE}...")
    long_df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")

if __name__ == "__main__":
    main()# ingest/backfill/unpivot_to_long.py