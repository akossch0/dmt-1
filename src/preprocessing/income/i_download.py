

import os
import re
import requests
from pathlib import Path
from tqdm import tqdm

# ─────────── Config ───────────
DATASETS = {
    "renda-disponible-llars-bcn": {
        "output_dir": Path("../../../data/income/raw"),
        "allowed_formats": {"csv"},
        "name_filter": None,
        "rename_to": "income.csv"
    },
    "20170706-districtes-barris": {
        "output_dir": Path("../../../data/income/raw"),
        "allowed_formats": {"json"},
        "name_filter": "barris",
        "rename_to": "BarcelonaCiutat_Barris.json"
    }
}
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
    cd = r.headers.get("Content-Disposition", "")
    m  = re.search(r'filename="?([^";]+)"?', cd, flags=re.I)
    return m.group(1) if m else fallback

# ------------- main ----------------

def download_latest_resource(dataset_id: str,
                             output_dir: Path,
                             allowed_formats: set[str],
                             name_filter: str | None = None,
                             rename_to: str | None = None) -> None:

    meta      = fetch_dataset_metadata(dataset_id)
    resources = meta.get("resources", [])

    os.makedirs(output_dir, exist_ok=True)

    for res in resources:
        if res.get("url") and res.get("format", "").lower() in allowed_formats:
            name = res.get("name", "").lower()
            url  = res["url"].lower()

            if name_filter and (name_filter not in name and name_filter not in url):
                continue

            fmt      = res["format"].lower()
            proto_fn = sanitize(res["name"])

            try:
                resp = requests.get(res["url"], stream=True, timeout=60)
                resp.raise_for_status()
            except Exception as exc:
                print(f"[FAIL] {res['url']} — {exc}")
                return

            # Determine final file name
            if rename_to:
                real_fn = rename_to
            else:
                original_fn = pick_filename(resp, proto_fn)
                real_fn = sanitize(original_fn)

            dest = output_dir / real_fn
            if dest.exists():
                print(f"[SKIP] {real_fn} already downloaded")
                resp.close()
                return

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

            print(f"[ OK ] Saved to {dest}")
            return
    print(f"[FAIL] No suitable resource found for dataset {dataset_id}.")

if __name__ == "__main__":
    for dataset_id, config in DATASETS.items():
        print(f"→ Downloading from dataset: {dataset_id}")
        download_latest_resource(
            dataset_id      = dataset_id,
            output_dir      = config["output_dir"],
            allowed_formats = config["allowed_formats"],
            name_filter     = config.get("name_filter"),
            rename_to       = config.get("rename_to"),
        )
