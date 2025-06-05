""" """

import os

import scrapy
from scrapy.spiders import Spider
from tqdm import tqdm

from ..items import FarmSubsidyItem


class NLSpider(Spider):
    name = "NL"
    year = 2022
    # Use absolute path based on this file's location
    data_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../special-scrapers/data/nl")
    )

    custom_settings = {
        # "ITEM_PIPELINES": {
        #     "scrapy_fs.pipelines.DuplicatesPipeline": 100,
        # },
        "LOG_LEVEL": "INFO",
        "AUTOTHROTTLE_ENABLED": False,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def start_requests(self):
        for file in tqdm(os.listdir(self.data_dir)):
            if file.endswith(".html"):
                file_path = os.path.join(self.data_dir, file)
                url = "file:///" + file_path
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Extract table rows from the HTML
        rows = response.css(
            "tbody#_EuSubsidies_WAR_EuSubsidiesportlet_\\:subsidiesForm\\:glbTable_data tr"
        )

        for row in rows:
            # Extract recipient name and location from the first column
            name_cell = row.css("td:first-child ul li::text").getall()
            if len(name_cell) >= 2:
                recipient_name = name_cell[0].strip()
                recipient_location = name_cell[1].strip()

                # Extract postcode and city from location
                location_parts = recipient_location.split(" ", 1)
                postcode = location_parts[0] if location_parts else ""
                city = location_parts[1] if len(location_parts) > 1 else ""
            else:
                recipient_name = ""
                recipient_location = ""
                postcode = ""
                city = ""

            # Extract scheme name from second column
            # The HTML structure is: <td><span>Regeling</span>SCHEME_CODE. SCHEME_DESCRIPTION</td>
            scheme_cell = row.css("td:nth-child(2)")
            if scheme_cell:
                # Get all text content from the cell
                all_text = scheme_cell.css("::text").getall()
                # Filter out the "Regeling" span text and join the rest
                scheme_full = "".join(
                    [text for text in all_text if text.strip() != "Regeling"]
                ).strip()

                # Split scheme into code and description
                # Look for pattern like "II.04. " or "IV.22. "
                if ". " in scheme_full:
                    parts = scheme_full.split(". ", 1)
                    scheme_code = parts[0] + "."
                    scheme_name = " ".join(parts[1].split()) if len(parts) > 1 else ""
                else:
                    scheme_code = ""
                    scheme_name = " ".join(scheme_full.split())
            else:
                scheme_full = ""
                scheme_code = ""
                scheme_name = ""

            # Extract amount from third column
            amount_text = row.css("td:nth-child(3) span.amount::text").getall()
            if amount_text:
                # Join the amount parts and clean up
                amount_str = "".join(amount_text).strip()
                # Remove € symbol and convert comma to dot for decimal
                amount_str = (
                    amount_str.replace("€", "")
                    .replace(".", "")
                    .replace(",", ".")
                    .strip()
                )
                try:
                    amount = float(amount_str)
                except ValueError:
                    amount = 0.0
            else:
                amount = 0.0

            if amount == 0.0:
                # Skip rows with zero amount
                raise ValueError(
                    f"Skipping row with zero amount: {recipient_name}, {scheme_name}"
                )

            # Create and yield the item
            item = FarmSubsidyItem()
            item["year"] = self.year
            item["country"] = "NL"
            item["recipient_name"] = recipient_name
            item["recipient_location"] = city
            item["recipient_postcode"] = postcode
            item["scheme_name"] = scheme_name
            item["scheme_code"] = scheme_code
            item["amount"] = amount
            item["currency"] = "EUR"

            yield item
