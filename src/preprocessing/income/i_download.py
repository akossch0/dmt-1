
import os
import re
import requests
from pathlib import Path
from tqdm import tqdm
import pandas as pd

# ─────────── Config ───────────
DATASET_ID = "renda-disponible-llars-bcn"
OUTPUT_DIR = Path("../../../data/income/raw")
ALLOWED_FORMATS = {"csv"}
# ──────────────────────────────

def fetch_dataset_metadata(dataset_id: str) -> dict:
    url = ("https://opendata-ajuntament.barcelona.cat/data/api/3/action/"
           f"package_show?id={dataset_id}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    meta = r.json()
    if not meta.get("success", False):
        raise RuntimeError("CKAN returned success=false")
    return meta["result"]

def extract_year_from_name(name: str) -> str:
    match = re.search(r"(20\d{2})", name)
    return match.group(1) if match else "unknown"

def download_all_csv_resources(dataset_id: str,
                                output_dir: Path,
                                allowed_formats: set[str]) -> None:

    meta = fetch_dataset_metadata(dataset_id)
    resources = meta.get("resources", [])

    os.makedirs(output_dir, exist_ok=True)

    for res in resources:
        if res.get("url") and res.get("format", "").lower() in allowed_formats:
            name = res.get("name", "")
            url = res["url"]
            year = extract_year_from_name(name)

            try:
                resp = requests.get(url, stream=True, timeout=60)
                resp.raise_for_status()
            except Exception as exc:
                print(f"[FAIL] {url} — {exc}")
                continue

            try:
                df = pd.read_csv(resp.raw, sep=",", encoding="utf-8")
                if "Any" in df.columns:
                    df["Any"] = pd.to_datetime(df["Any"].astype(str) + "-01-01").dt.strftime("%Y-%m-%d")
                out_path = output_dir / f"income_{year}.csv"
                df.to_csv(out_path, sep=",", index=False, encoding="utf-8")
                print(f"[ OK ] Saved to {out_path}")
            except Exception as e:
                print(f"[FAIL] Error reading CSV from {url}: {e}")
                continue

if __name__ == "__main__":
    download_all_csv_resources(
        dataset_id=DATASET_ID,
        output_dir=OUTPUT_DIR,
        allowed_formats=ALLOWED_FORMATS,
    )