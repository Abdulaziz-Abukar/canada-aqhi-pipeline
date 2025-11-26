from pathlib import Path
import csv

# Adjust these to your actual structure
RAW_DIR = Path(r"C:\Data Projects\canada-aqhi-pipeline\data\raw")
OUTPUT_DIR = Path(r"C:\Data Projects\canada-aqhi-pipeline\data\processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "aqhi_pnr_2023_2025_combined.csv"


def combined_csvs():
    csv_files = sorted(RAW_DIR.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {RAW_DIR}")
        return

    print(f"Found {len(csv_files)} CSV files. Combining...")

    # ---------- 1st pass: build a unified header ----------
    header = []
    seen = set()

    for file_path in csv_files:
        with file_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue

            for field in reader.fieldnames:
                if field not in seen:
                    seen.add(field)
                    header.append(field)

    # add our extra column at the end
    header.append("source_file")

    # ---------- 2nd pass: write combined file ----------
    rows_written = 0

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=header)
        writer.writeheader()

        for file_path in csv_files:
            print(f"processing {file_path.name}...")
            with file_path.open("r", newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)

                for row in reader:
                    # ensure we only pass known keys to writer (handles any weird extras)
                    safe_row = {key: row.get(key, "") for key in header if key != "source_file"}
                    safe_row["source_file"] = file_path.name
                    writer.writerow(safe_row)
                    rows_written += 1

    print(f"Combined {len(csv_files)} files into {OUTPUT_FILE} with {rows_written} total rows.")


if __name__ == "__main__":
    combined_csvs()
