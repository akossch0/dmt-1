import csv
import logging
from pathlib import Path
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

NEEDED_COLUMNS = {
    "information": ["station_id", "name", "lat", "lon", "altitude", "capacity", "last_updated"],
    "status": ["station_id", "num_bikes_available", "num_bikes_available_types.mechanical", 
               "num_bikes_available_types.ebike", "num_docks_available", "last_reported", 
               "status", "last_updated"]
}

BASE_PATH = Path("data")


def ensure_output_dir(file_path: Path, input_dir: Path, output_dir: Path) -> Path:
    """Create output directory structure mirroring the input structure."""
    rel_path = file_path.relative_to(input_dir)
    target_dir = output_dir / rel_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / file_path.name


def process_csv_file(input_file: Path, input_dir: Path, output_dir: Path, columns_to_keep: list):
    output_file = ensure_output_dir(input_file, input_dir, output_dir)
    
    if output_file.exists():
        log.info(f"[SKIP] Already processed: {input_file}")
        return True
    
    log.info(f"Processing: {input_file}")
    log.info(f"Output to: {output_file}")
    
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            # Read the header first to determine column indices
            reader = csv.reader(infile)
            header = next(reader)
            
            # Find indices of columns we want to keep
            column_indices = {}
            for i, col_name in enumerate(header):
                if col_name in columns_to_keep:
                    column_indices[i] = columns_to_keep.index(col_name)
            
            # Check if we found all needed columns
            found_columns = set(col_name for i, col_name in enumerate(header) if i in column_indices)
            missing_columns = set(columns_to_keep) - found_columns
            if missing_columns:
                log.warning(f"Missing columns in {input_file}: {missing_columns}")
            
            # Create a new header with only the columns we need (in the original order)
            new_header = [None] * len(columns_to_keep)
            for idx, new_idx in column_indices.items():
                new_header[new_idx] = header[idx]
            
            # Filter out None values (for columns that weren't found)
            new_header = [h for h in new_header if h is not None]
            
            # Write the new CSV with only the needed columns
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(new_header)
                
                # Process rows in chunks to avoid loading entire file
                rows_processed = 0
                for row in reader:
                    new_row = [None] * len(new_header)
                    for idx, new_idx in column_indices.items():
                        if idx < len(row):  # Ensure index is valid
                            col_position = new_idx
                            if col_position < len(new_header):
                                new_row[col_position] = row[idx]
                    
                    # Filter out None values (for columns that weren't found)
                    new_row = [val for i, val in enumerate(new_row) if i < len(new_header)]
                    writer.writerow(new_row)
                    
                    rows_processed += 1
                    if rows_processed % 1000000 == 0:
                        log.info(f"Processed {rows_processed} rows of {input_file.name}")
        
        log.info(f"Completed: {input_file.name} - Processed {rows_processed} rows")
        return True
    
    except Exception as e:
        log.error(f"Error processing {input_file}: {e}")
        return False


def process_directory(data_type: str):
    """Process all CSV files in a directory based on data type."""
    input_dir = BASE_PATH / f"bicycle_stations/{data_type}/decompressed"
    output_dir = BASE_PATH / f"bicycle_stations/{data_type}/projected"
    
    # Ensure base output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files recursively
    all_files = list(input_dir.glob("**/*.csv"))
    log.info(f"Found {len(all_files)} CSV files to process for {data_type}")
    
    # Process each file
    success_count = 0
    for file in tqdm(all_files, desc=f"Processing {data_type} files"):
        if process_csv_file(file, input_dir, output_dir, NEEDED_COLUMNS[data_type]):
            success_count += 1
    
    log.info(f"Processing complete for {data_type}. Successfully processed {success_count} of {len(all_files)} files.")


if __name__ == "__main__":
    data_types = ["information", "status"]
    
    for data_type in data_types:
        print(f"\n=== Processing {data_type} data ===")
        log.info(f"Starting projection of bicycle station {data_type} data")
        process_directory(data_type)
        log.info(f"Projection of {data_type} data completed") 