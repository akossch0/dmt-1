import os
import re
import requests
from pathlib import Path
from tqdm import tqdm

# ─────────── Config ───────────
DATASET_ID   = "20170706-districtes-barris"
OUTPUT_DIR   = Path("data/administrative_units/raw")
ALLOWED_FORMATS = {"shp"}                
# ──────────────────────────────


# ------------ helpers -------------

def fetch_dataset_metadata(dataset_id: str) -> dict:
    url = ("https://opendata-ajuntament.barcelona.cat/data/api/3/action/"
           f"package_show?id={dataset_id}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    meta = r.json()
    if not meta.get("success", False):
        raise RuntimeError("CKAN returned success=false")
    return meta["result"]


def sanitize(name: str) -> str:
    return name.replace("/", "_").replace(" ", "_")


def pick_filename(r, fallback: str) -> str:
    """
    Extract a filename from Content-Disposition if present;
    otherwise return *fallback*.
    """
    cd = r.headers.get("Content-Disposition", "")
    m  = re.search(r'filename="?([^";]+)"?', cd, flags=re.I)
    return m.group(1) if m else fallback


# ------------- main ----------------

def download_resources(dataset_id: str,
                       output_dir: Path,
                       allowed_formats: set[str]) -> None:

    meta      = fetch_dataset_metadata(dataset_id)
    resources = meta.get("resources", [])

    os.makedirs(output_dir, exist_ok=True)

    # keep only desired formats (SHP here)
    resources = [r for r in resources
                 if r.get("url")
                 and r.get("format", "").lower() in allowed_formats]

    for res in tqdm(resources, desc="Processing resources", unit="file"):
        url      = res["url"]
        fmt      = res["format"].lower()          # shp / geojson / …
        proto_fn = sanitize(res["name"])          # Unitats_Administratives_BCN.shp

        # single request = get headers + file
        try:
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
        except Exception as exc:
            tqdm.write(f"[FAIL] {url} — {exc}")
            continue

        # ensure locally-unique filename: add _<format> before .zip
        original_zip = pick_filename(resp, proto_fn)         # BCN_UNITATS_ADM.zip
        stem         = Path(original_zip).stem               # BCN_UNITATS_ADM
        suffix       = Path(original_zip).suffix             # .zip
        real_fn      = sanitize(f"{stem}_{fmt}{suffix}")     # BCN_UNITATS_ADM_shp.zip

        dest = output_dir / real_fn
        if dest.exists():
            tqdm.write(f"[SKIP] {real_fn} already downloaded")
            resp.close()
            continue

        total = int(resp.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            desc=f"Downloading {real_fn}",
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
        ) as bar:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

        tqdm.write(f"[ OK ] Saved to {dest}")


if __name__ == "__main__":
    download_resources(
        dataset_id      = DATASET_ID,
        output_dir      = OUTPUT_DIR,
        allowed_formats = ALLOWED_FORMATS,
    )