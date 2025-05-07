import zipfile
import py7zr
from pathlib import Path
from tqdm import tqdm


def decompress_archive(file_path: Path, dest_dir: Path):
    dest_dir.mkdir(parents=True, exist_ok=True)

    if file_path.suffix == ".zip":
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(dest_dir)

    elif file_path.suffix == ".7z":
        with py7zr.SevenZipFile(file_path, mode='r') as archive:
            archive.extractall(path=dest_dir)

    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")


def decompress_all(raw_root: Path):
    files_to_decompress = [f for f in raw_root.rglob("*") if f.suffix in [".zip", ".7z"]]
    for raw_file in tqdm(files_to_decompress, desc=f"Decompressing files in {raw_root}", unit="file"):
        relative = raw_file.relative_to(raw_root)
        out_folder = raw_root.parent / "decompressed" / relative.with_suffix("")

        if out_folder.exists() and any(out_folder.iterdir()):
            tqdm.write(f"[SKIP] Already decompressed: {relative}")
            continue

        try:
            tqdm.write(f"[DECOMPRESSING] {relative}")
            decompress_archive(raw_file, out_folder)
            tqdm.write(f"[SUCCESS] Extracted to: {out_folder}")
        except Exception as e:
            tqdm.write(f"[ERROR] Failed to decompress {relative} -> {e}")


if __name__ == "__main__":
    base_dirs = [
        Path("data/bicycle_stations/status/raw"),
        Path("data/bicycle_stations/information/raw"),
        Path("data/bicycle_lanes/raw")
    ]

    for raw_dir in base_dirs:
        print(f"\n=== Processing directory: {raw_dir} ===")
        decompress_all(raw_dir)
