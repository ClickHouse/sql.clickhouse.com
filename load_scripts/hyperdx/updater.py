#!/usr/bin/env python3
import tarfile
import os
import sys
import time
from datetime import datetime
import re

# Regex patterns for numeric timestamps (rough heuristics)
TIMESTAMP_PATTERN = re.compile(r'\b(17476\d{8,15})\b')

# doesn't need to be exact but we take a min-ish number
MIN_NANO=1747670083391674600


def process_telemetry_file(input_filepath, min_nano, now, output_filepath=None):
    if not output_filepath:
        output_dir = os.path.dirname(input_filepath)
        output_filepath = os.path.join(output_dir, f"updated_{os.path.basename(input_filepath)}")

    print(f"Processing {input_filepath}...")
    if min_nano == float('inf'):
        print("Error: No nanosecond timestamp found.")
        return False

    # Step 2: Compute offsets

    offsets = {
        'nano': now * 1_000_000_000 - min_nano,
        'micro': (now * 1_000_000) - min_nano // 1_000,
        'milli': (now * 1_000) - min_nano // 1_000_000,
    }

    print(f"Minimum nanosecond timestamp: {min_nano}")
    print(f"Offsets → ns: {offsets['nano']}, µs: {offsets['micro']}, ms: {offsets['milli']}")
    print(f"Target time: {datetime.fromtimestamp(now)}")

    # Step 3: Second pass to apply offsets
    updated_lines = []
    with open(input_filepath, 'r') as f:
        for line in f:
            def replace(match):
                val = int(match.group(1))
                if val >= 1000000000000000:  # 1747678698470325528
                    return str(val + offsets['nano'])  # Nanoseconds
                elif val >= 10000000000000:  # 1747670903666
                    return str(val + offsets['micro'])  # Microseconds
                elif val >= 10000000000:
                    return str(val + offsets['milli'])  # Milliseconds
                else:
                    return match.group(0)

            updated_line = TIMESTAMP_PATTERN.sub(replace, line)
            updated_lines.append(updated_line)

    # Step 4: Save
    with open(output_filepath, 'w') as f:
        f.writelines(updated_lines)

    print(f"✅ Updated file saved to {output_filepath}")
    return True

def decompress_tar_gz(filepath):
    """
    Decompress a .tar.gz file to a 'sample' subdirectory of the current directory.
    """
    try:
        if not os.path.exists(filepath):
            print(f"Error: File {filepath} does not exist")
            return False

        extract_dir = os.path.join(os.path.dirname(filepath), "sample")
        os.makedirs(extract_dir, exist_ok=True)

        print(f"Extracting {filepath} to {extract_dir}...")
        with tarfile.open(filepath, "r:gz") as tar:
            tar.extractall(path=extract_dir)

        expected_files = ["logs.json", "traces.json", "metrics.json"]
        found_files = os.listdir(extract_dir)
        missing_files = [f for f in expected_files if f not in found_files]

        if missing_files:
            print(f"Warning: The following expected files were not found: {', '.join(missing_files)}")
        else:
            print(f"Successfully extracted all expected files to {extract_dir}/")

        return True

    except tarfile.ReadError:
        print(f"Error: {filepath} is not a valid tar.gz file")
        return False
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "source.tar.gz"

    success = decompress_tar_gz(file_path)
    if success:
        extracted_dir = os.path.join(os.path.dirname(file_path), "sample")

        logs_json = os.path.join(extracted_dir, "logs.json")
        traces_json = os.path.join(extracted_dir, "traces.json")
        metrics_json = os.path.join(extracted_dir, "metrics.json")
        now = int(time.time()) - (5 * 60)
        for f in [logs_json, traces_json, metrics_json]:
            process_telemetry_file(f, MIN_NANO, now, output_filepath=f + ".new")

        # Overwrite originals with .new files
        for f in [logs_json, traces_json, metrics_json]:
            new_f = f + ".new"
            if os.path.exists(new_f):
                os.replace(new_f, f)

        # Create new tar.gz archive from 'sample' dir
        output_tar = os.path.join(os.path.dirname(file_path), "sample.tar.gz")
        with tarfile.open(output_tar, "w:gz") as tar:
            for name in ["logs.json", "traces.json", "metrics.json"]:
                file_path_to_add = os.path.join(extracted_dir, name)
                if os.path.exists(file_path_to_add):
                    tar.add(file_path_to_add, arcname=name)

        print(f"Created updated archive: {output_tar}")
        sys.exit(0)
    else:
        sys.exit(1)
