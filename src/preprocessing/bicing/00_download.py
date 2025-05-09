import os
import requests
from pathlib import Path
from typing import Optional
from tqdm import tqdm


def fetch_dataset_metadata(dataset_id: str) -> dict:
    api_url = f"https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_show?id={dataset_id}"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    if not data.get("success", False):
        raise RuntimeError("Failed to fetch dataset metadata.")
    return data["result"]


def sanitize_filename(name: str) -> str:
    return name.replace("/", "_").replace(" ", "_")


def download_file(url: str, dest_path: Path) -> None:
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    with open(dest_path, "wb") as f, tqdm(
        desc=f"Downloading {dest_path.name}",
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
        leave=False
    ) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                bar.update(len(chunk))


def download_resources_from_metadata(
    dataset_id: str,
    output_dir: Path,
    filter_years: Optional[range] = None,
    allowed_extensions: Optional[tuple] = (".7z", ".zip", ".csv"),
) -> None:
    metadata = fetch_dataset_metadata(dataset_id)
    resources = metadata.get("resources", [])

    os.makedirs(output_dir, exist_ok=True)

    filtered_resources = [
        res for res in resources
        if res.get("url") and any(res.get("name", "").endswith(ext) for ext in allowed_extensions)
           and (not filter_years or any(str(year) in res["name"] for year in filter_years))
    ]

    for res in tqdm(filtered_resources, desc=f"Processing dataset {dataset_id}", unit="file"):
        name = sanitize_filename(res["name"])
        url = res["url"]
        dest_file = output_dir / name

        if dest_file.exists():
            tqdm.write(f"[SKIP] Already downloaded: {name}")
            continue

        try:
            download_file(url, dest_file)
            tqdm.write(f"[SUCCESS] Saved to: {dest_file}")
        except Exception as e:
            tqdm.write(f"[ERROR] Failed to download {url} -> {e}")

if __name__ == "__main__":
    filter_years = range(2019, 2026)
    datasets = [
        {
            "id": "6aa3416d-ce1a-494d-861b-7bd07f069600",  # Station status
            "output": Path("data/bicycle_stations/status/raw"),
            "filter_years": filter_years
        },
        {
            "id": "bd2462df-6e1e-4e37-8205-a4b8e7313b84",  # Station info
            "output": Path("data/bicycle_stations/information/raw"),
            "filter_years": filter_years
        },
        {
            "id": "e3497ea4-0bae-4093-94a7-119df50a8a74",  # Bicycle lanes
            "output": Path("data/bicycle_lanes/raw"),
            "filter_years": filter_years
        }
    ]

    for dataset in datasets:
        print(f"\n=== Processing dataset: {dataset['id']} ===")
        download_resources_from_metadata(
            dataset_id=dataset["id"],
            output_dir=dataset["output"],
            filter_years=dataset["filter_years"],
            allowed_extensions=(".7z", ".csv", ".zip", ".xls", ".xlsx", ".geojson")
        )
