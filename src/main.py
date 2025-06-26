import re
import os
import json
import requests
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor


OUTPUT_DIR = Path("cms_hospitals_data")
METADATA_FILE = OUTPUT_DIR / "metadata.json"
SOURCE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"


def snake_case(name):
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)  # remove special characters but keep spaces
    name = re.sub(r"\s+", "_", name)  # replace one or more spaces with underscore
    return name.strip("_")


def load_metadata():
    if METADATA_FILE.exists():
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {}


def save_metadata(metadata):
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)


def fetch_hospital_datasets():
    response = requests.get(SOURCE_URL)
    response.raise_for_status()
    data = response.json()
    return [d for d in data if 'Hospitals' in d.get('theme', '')]


def download_and_process(dataset, last_metadata):
    title = dataset['title']
    modified = dataset.get('modified')
    download_url = dataset.get('distribution', [{}])[0].get('downloadURL')
    file_path = urlparse(download_url).path
    name = Path(file_path).name

    # Skip if not updated
    if name in last_metadata and last_metadata[name] == modified:
        print(f"Skipping {name} (unchanged)")
        return None

    try:
        print(f"Downloading: {title} from {download_url}")
        df = pd.read_csv(download_url, low_memory=False)

        # Convert column names
        df.columns = [snake_case(col) for col in df.columns]

        # Save CSV
        output_path = OUTPUT_DIR / f"{name}"
        df.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")

        return name, modified
    except Exception as e:
        print(f"Failed to process {name}: {e}")
        return None


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    last_metadata = load_metadata()
    updated_metadata = last_metadata.copy()

    datasets = fetch_hospital_datasets()

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(download_and_process, d, last_metadata) for d in datasets]
        for future in futures:
            result = future.result()
            if result:
                name, modified = result
                updated_metadata[name] = modified

    save_metadata(updated_metadata)


if __name__ == "__main__":
    main()
