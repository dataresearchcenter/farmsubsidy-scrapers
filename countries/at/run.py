"""
Austria
"""

import csv
import gzip
import json
import os
from pathlib import Path

from tqdm import tqdm

from utils.download import download_file

BASE_URL = "https://www.transparenz.at/export/Export_%s.zip"
DATA_FILE = "Daten.json"
SCHEMA_FILE = "Massnahmen.json"

mappings = {
    "hhj": "year",
    "laufnr": "recipient_id",
    "zahlungsempfaenger": "recipient_name",
    "postleitzahl": "recipient_postcode",
    "gemeinde": "recipient_location",
}

payment_mappings = {
    "bezeichnung": "scheme_name",
    "textcode": "scheme_code",
    "beschreibung": "scheme_description",
}


def run(year, data_dir):
    """
    Austria data download
    """
    url = BASE_URL % year
    dl_dir = os.path.join(data_dir, "raw", "at", year)
    done_dir = os.path.join(data_dir, "done", "at", year)
    download_file(url, dl_dir)

    # prepare schema dict
    schema_raw = json.loads(
        (Path(dl_dir) / "extracted" / SCHEMA_FILE).read_text(encoding="UTF-8"),
    )
    schema = {}
    for row in schema_raw:
        row_fixed = row.copy()
        del row_fixed["textid"]
        # apply payment_mappings
        for k, v in payment_mappings.items():
            row_fixed[v] = row_fixed[k]
            del row_fixed[k]

        schema[row["textid"]] = row_fixed

    raw_data = json.loads(
        (Path(dl_dir) / "extracted" / DATA_FILE).read_text(encoding="UTF-8"),
    )

    base_row_data = {"year": year, "country": "AT", "currency": "EUR"}

    Path(done_dir).mkdir(parents=True, exist_ok=True)
    with gzip.open(Path(done_dir) / "data.csv.gz", "wt") as f:
        fns = (
            list(mappings.values())
            + list(base_row_data.keys())
            + list(payment_mappings.values())
            + ["amount"]
        )

        writer = csv.DictWriter(f, fieldnames=fns)
        writer.writeheader()

        for row in tqdm(raw_data, desc="Processing data"):
            row_base_done = base_row_data.copy()
            for k, v in mappings.items():
                row_base_done[v] = row[k]

            for payment in row["zahlungen"]:
                row_done = row_base_done.copy()
                row_done["amount"] = (
                    payment["betragEgfl"]
                    + payment["betragEler"]
                    + payment["betragKofinanzierung"]
                )
                row_done.update(schema[payment["textid"]])

                writer.writerow(row_done)
