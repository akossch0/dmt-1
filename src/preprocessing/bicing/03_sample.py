import csv
import logging
import os
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Paths
BASE_PATH = Path("data")


def ensure_output_dir(file_path: Path, input_dir: Path, output_dir: Path) -> Path:
    """Create output directory structure mirroring the input structure."""
    # Get relative path from input_dir
    rel_path = file_path.relative_to(input_dir)
    # Create the same directory structure in output_dir
    target_dir = output_dir / rel_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / file_path.name


def parse_timestamp(timestamp_str, row_num):
    """Parse timestamp from string in various formats."""
    # First try to parse as Unix timestamp (seconds since 1970-01-01)
    try:
        unix_timestamp = int(timestamp_str)
        return datetime.fromtimestamp(unix_timestamp)
    except ValueError:
        pass
    
    # Try other common formats
    formats_to_try = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO format with microseconds
        "%Y-%m-%dT%H:%M:%SZ",     # ISO format without microseconds
        "%Y-%m-%d %H:%M:%S",      # Standard format
        "%Y-%m-%d %H:%M:%S.%f",   # Standard format with microseconds
        "%d/%m/%Y %H:%M"          # European format dd/mm/yyyy HH:MM
    ]
    
    for fmt in formats_to_try:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    # If we got here, none of the formats matched
    if row_num <= 10 or row_num % 1000000 == 0:
        log.warning(f"Could not parse timestamp: '{timestamp_str}' at row {row_num}")
    return None


def sample_csv_file(input_file: Path, input_dir: Path, output_dir: Path, data_type: str):
    """Sample a CSV file based on timestamps.
    Information data: one per hour
    Status data: one per 10 minutes
    """
    # Skip if output file already exists
    output_file = ensure_output_dir(input_file, input_dir, output_dir)
    if output_file.exists():
        log.info(f"[SKIP] Already sampled: {input_file}")
        return True
    
    log.info(f"Sampling: {input_file}")
    log.info(f"Output to: {output_file}")
    
    try:
        # Initialize tracking variables
        rows_processed = 0
        rows_kept = 0
        rows_skipped = 0
        last_hour_seen = {}        # station_id -> (hour, day, month, year)
        last_ten_min_seen = {}     # station_id -> (ten_min_interval, hour, day, month, year)
        first_rows = []            # Store first 10 rows for debugging
        
        # Open input file and read header
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            log.info(f"Header: {header}")
            
            # Find important column indices
            timestamp_col_idx = None
            station_id_idx = None
            
            for i, col_name in enumerate(header):
                if col_name.lower() == "last_updated" or col_name.lower() == "last_reported":
                    timestamp_col_idx = i
                    log.info(f"Found timestamp column: {col_name} at index {i}")
                if col_name.lower() == "station_id":
                    station_id_idx = i
                    log.info(f"Found station_id column at index {i}")
            
            if timestamp_col_idx is None:
                log.warning(f"No timestamp column found in {input_file}. Will not sample by time.")
            
            if station_id_idx is None and data_type == "information":
                log.warning(f"No station_id column found in {input_file}. Will use row number as station ID.")
            
            # Open output file and write header
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)
                
                # Process rows
                for row in reader:
                    rows_processed += 1
                    
                    # Collect sample rows for debugging
                    if rows_processed <= 10:
                        first_rows.append(row)
                    
                    # Default to keeping row
                    keep_row = True
                    
                    # Get station ID
                    station_id = str(rows_processed)  # Default to row number
                    if station_id_idx is not None and station_id_idx < len(row):
                        station_id = row[station_id_idx]
                    
                    # Skip timestamp handling if no timestamp column
                    if timestamp_col_idx is None or timestamp_col_idx >= len(row):
                        writer.writerow(row)
                        rows_kept += 1
                        continue
                    
                    # Get timestamp value
                    timestamp_str = row[timestamp_col_idx]
                    
                    # Parse timestamp
                    try:
                        timestamp = parse_timestamp(timestamp_str, rows_processed)
                        
                        # If timestamp is invalid, keep the row
                        if timestamp is None:
                            writer.writerow(row)
                            rows_kept += 1
                            continue
                        
                        # Extract time components
                        hour = timestamp.hour
                        day = timestamp.day
                        month = timestamp.month
                        year = timestamp.year
                        minute = timestamp.minute
                        
                        # Apply sampling logic based on data type
                        if data_type == "status":
                            # For status data: one per 10 minutes
                            ten_min_interval = minute // 10
                            interval_key = (ten_min_interval, hour, day, month, year)
                            
                            if station_id in last_ten_min_seen and last_ten_min_seen[station_id] == interval_key:
                                # Skip this row - we already have data for this station in this interval
                                keep_row = False
                                rows_skipped += 1
                            else:
                                # Keep this row and update tracking
                                last_ten_min_seen[station_id] = interval_key
                                
                        elif data_type == "information":
                            # For information data: one per hour
                            hour_key = (hour, day, month, year)
                            
                            if station_id in last_hour_seen and last_hour_seen[station_id] == hour_key:
                                # Skip this row - we already have data for this station in this hour
                                keep_row = False
                                rows_skipped += 1
                            else:
                                # Keep this row and update tracking
                                last_hour_seen[station_id] = hour_key
                    
                    except Exception as e:
                        # If there's an error processing the timestamp, keep the row
                        if rows_processed <= 10 or rows_processed % 1000000 == 0:
                            log.warning(f"Error processing timestamp in row {rows_processed}: {e}")
                        keep_row = True
                    
                    # Write row if we're keeping it
                    if keep_row:
                        writer.writerow(row)
                        rows_kept += 1
                    
                    # Progress reporting
                    if rows_processed % 1000000 == 0:
                        log.info(f"Processed {rows_processed} rows, kept {rows_kept} rows, skipped {rows_skipped} rows")
        
        # Report final statistics
        reduction = 100 - (rows_kept / rows_processed * 100) if rows_processed > 0 else 0
        log.info(f"Completed: {input_file.name} - Processed {rows_processed} rows, kept {rows_kept} rows, skipped {rows_skipped} rows ({reduction:.1f}% reduction)")
        return True
        
    except Exception as e:
        log.error(f"Error sampling {input_file}: {e}")
        return False


def sample_directory(data_type: str):
    """Sample all CSV files in a directory based on data type."""
    input_dir = BASE_PATH / f"bicycle_stations/{data_type}/projected"
    output_dir = BASE_PATH / f"bicycle_stations/{data_type}/sampled"
    
    # Ensure base output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files recursively
    all_files = list(input_dir.glob("**/*.csv"))
    log.info(f"Found {len(all_files)} CSV files to sample for {data_type}")
    
    # Process each file
    success_count = 0
    for file in tqdm(all_files, desc=f"Sampling {data_type} files"):
        if sample_csv_file(file, input_dir, output_dir, data_type):
            success_count += 1
    
    log.info(f"Sampling complete for {data_type}. Successfully sampled {success_count} of {len(all_files)} files.")


if __name__ == "__main__":
    data_types = ["information", "status"]
    
    for data_type in data_types:
        print(f"\n=== Sampling {data_type} data ===")
        log.info(f"Starting sampling of bicycle station {data_type} data")
        sample_directory(data_type)
        log.info(f"Sampling of {data_type} data completed") 