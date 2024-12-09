"""
Download a file from a URL and extract it to a folder.
"""

import os
import pathlib
import shutil

import requests
from tqdm import tqdm

CHUNK_SIZE = 1024 * 8
TIMEOUT = 10


def download_file(url, folder):
    """
    Download a file from a URL and extract it to a folder.
    """
    print(f"Downloading from {url}")
    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)
    local_filename = url.split("/")[-1]
    fn = os.path.join(folder, local_filename)
    extract_folder = os.path.join(folder, "extracted")

    try:
        if not os.path.exists(fn):
            with requests.get(url, stream=True, timeout=TIMEOUT) as r:
                r.raise_for_status()
                total_len = r.headers.get("content-length", None)
                total = int(total_len) / CHUNK_SIZE if total_len is not None else None

                with open(fn, "wb") as f:
                    for chunk in tqdm(
                        r.iter_content(chunk_size=CHUNK_SIZE),
                        total=total,
                        unit="KB",
                        desc=local_filename,
                        unit_scale=True,
                    ):
                        # If you have chunk encoded response uncomment if
                        # and set chunk_size parameter to None.
                        # if chunk:
                        f.write(chunk)
        shutil.unpack_archive(fn, extract_folder)
    except Exception as e:
        shutil.rmtree(folder)
        raise e
    print(f"Successfully downloaded to {folder}")
    return local_filename
