# admin_units_decompress.py
"""Unpack **only** the *BCN_UNITATS_ADM.zip* archive that lives in
*data/administrative_units/raw* (ignore any older ZIPs that might be sitting
next to it).

The archive is extracted into a sibling directory:

```
 data/administrative_units/
   ├─ raw/BCN_UNITATS_ADM.zip
   └─ decompressed/BCN_UNITATS_ADM/  <-- here
```
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import py7zr
from tqdm import tqdm

RAW_ROOT  = Path("data/administrative_units/raw")
TARGET_ZIP = "BCN_UNITATS_ADM_shp.zip"      # case-insensitive match

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def decompress_archive(archive: Path, dest_dir: Path) -> None:
    """Extract *archive* into *dest_dir* (creates the folder if missing)."""
    dest_dir.mkdir(parents=True, exist_ok=True)

    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive, "r") as zf:
            zf.extractall(dest_dir)
    elif archive.suffix == ".7z":
        with py7zr.SevenZipFile(archive, mode="r") as zf:
            zf.extractall(path=dest_dir)
    else:
        raise ValueError(f"Unsupported archive type: {archive.suffix}")


# ─────────────────────────────────────────────────────────────────────────────
# Main routine
# ─────────────────────────────────────────────────────────────────────────────

def decompress_bcn_units(raw_root: Path = RAW_ROOT, target_name: str = TARGET_ZIP) -> None:
    """Locate *target_name* inside *raw_root* (recursively) and extract it."""

    matches = [
        p for p in raw_root.rglob("*")
        if p.is_file() and p.name.lower() == target_name.lower()
    ]

    if not matches:
        tqdm.write(f"[WARN] No file named {target_name} found under {raw_root}")
        return

    for archive in matches:
        rel       = archive.relative_to(raw_root)  # e.g. BCN_UNITATS_ADM.zip
        out_dir   = raw_root.parent / "decompressed" / rel.with_suffix("")

        if out_dir.exists() and any(out_dir.iterdir()):
            tqdm.write(f"[SKIP] Already decompressed: {rel}")
            continue

        try:
            tqdm.write(f"[EXTRACT] {rel}")
            decompress_archive(archive, out_dir)
            tqdm.write(f"[ OK ] → {out_dir}")
        except Exception as exc:
            tqdm.write(f"[FAIL] {rel} — {exc}")


if __name__ == "__main__":
    decompress_bcn_units()
