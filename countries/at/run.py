"""
Austria
"""

import os

from utils.download import download_file

BASE_URL = "https://www.transparenz.at/export/Export_%s.zip"
DATA_FILE = "Daten.json"


def run(year, data_dir):
    """
    Austria data download
    """
    url = BASE_URL % year
    dl_dir = os.path.join(data_dir, "raw", "at", year)
    download_file(url, dl_dir)
