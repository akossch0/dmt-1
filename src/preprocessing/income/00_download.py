import os
import re
import requests
from pathlib import Path
from typing import Optional, Iterable
from tqdm import tqdm
import pandas as pd

# ─────────── Config ───────────
DATASET_ID       = "renda-disponible-llars-bcn"
OUTPUT_DIR       = Path("data/income/raw")
ALLOWED_FORMATS  = {"csv"}          # file formats to accept
FILTER_YEARS     = range(2019, 2022)  
# ──────────────────────────────


def fetch_dataset_metadata(dataset_id: str) -> dict:
    """Return the CKAN package metadata for a dataset."""
    url = (
        "https://opendata-ajuntament.barcelona.cat/data/api/3/action/"
        f"package_show?id={dataset_id}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    meta = r.json()
    if not meta.get("success", False):
        raise RuntimeError("CKAN returned success=false")
    return meta["result"]


def extract_year_from_name(name: str) -> str:
    """Extract the first 4-digit year (20xx) found in the resource name."""
    match = re.search(r"(20\d{2})", name)
    return match.group(1) if match else "unknown"


def year_filter(name: str, filter_years: Optional[Iterable[int]]) -> bool:
    """Return True if the resource name matches the allowed years (or if no filter)."""
    if not filter_years:
        return True
    return any(str(y) in name for y in filter_years)


def download_all_csv_resources(
    dataset_id: str,
    output_dir: Path,
    allowed_formats: set[str],
    filter_years: Optional[Iterable[int]] = None,
) -> None:
    """Download every CSV resource matching the optional year filter."""
    meta = fetch_dataset_metadata(dataset_id)
    resources = meta.get("resources", [])

    os.makedirs(output_dir, exist_ok=True)

    # Pre-filter once so tqdm shows the correct length
    eligible = [
        res for res in resources
        if (
            res.get("url")
            and res.get("format", "").lower() in allowed_formats
            and year_filter(res.get("name", ""), filter_years)
        )
    ]

    for res in tqdm(eligible, desc="Downloading resources", unit="file"):
        name = res.get("name", "")
        url = res["url"]
        year = extract_year_from_name(name)
        out_path = output_dir / f"income_{year}.csv"

        try:
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
        except Exception as exc:
            tqdm.write(f"[FAIL] {url} — {exc}")
            continue

        try:
            df = pd.read_csv(resp.raw, sep=",", encoding="utf-8")
            if "Any" in df.columns:
                df["Any"] = (
                    pd.to_datetime(df["Any"].astype(str) + "-01-01")
                    .dt.strftime("%Y-%m-%d")
                )
            df.to_csv(out_path, sep=",", index=False, encoding="utf-8")
            tqdm.write(f"[ OK ] Saved to {out_path}")
        except Exception as e:
            tqdm.write(f"[FAIL] Error reading CSV from {url}: {e}")
            continue


if __name__ == "__main__":
    download_all_csv_resources(
        dataset_id     = DATASET_ID,
        output_dir     = OUTPUT_DIR,
        allowed_formats= ALLOWED_FORMATS,
        filter_years   = FILTER_YEARS,   # ← pass None to disable filtering
    )
