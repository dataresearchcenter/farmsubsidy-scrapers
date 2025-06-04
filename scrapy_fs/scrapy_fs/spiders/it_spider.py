"""
Get data from all the HTML files scraped via Playwright in the `it` directory.

It is not possible to say whether the data is complete.
We had to narrow down the selection for each request to not overwhelm the server.
This was done by iterating through locations & measures.
But in the results, there were a lot of duplicated payments.
Maybe the search with the comune was too broad.
"""

import os

import scrapy
from scrapy.spiders import Spider
from tqdm import tqdm

from ..items import FarmSubsidyItem


class ITSpider(Spider):
    name = "IT"
    year = 2022
    # Use absolute path based on this file's location
    data_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../special-scrapers/data/it")
    )

    custom_settings = {
        "ITEM_PIPELINES": {
            "scrapy_fs.pipelines.DuplicatesPipeline": 100,
        },
        "LOG_LEVEL": "ERROR",
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
        # Loop through each table row that contains <td> elements
        for row in response.xpath("//tr[td]"):
            # Extract text from each <td> in the row
            cells = row.xpath("./td//text()").getall()
            # Clean up whitespace and join multi-line cell text
            cells = [cell.strip() for cell in cells if cell.strip()]
            if cells and cells[0] == "Beneficiary":
                continue

            offset = 0
            if cells and len(cells) < 5:
                offset = -2

            # create id from all cells since there are duplicate payments
            duplicate_id = "-".join(cells)
            recipient_id = "-".join([c for c in cells[:4]]).lower()
            recipient_id = "".join(
                [ch for ch in recipient_id if ch.isalnum() or ch == "-"]
            )
            # Create an item for each row
            item = FarmSubsidyItem()
            item["id"] = duplicate_id
            item["recipient_id"] = recipient_id
            item["recipient_name"] = cells[0]
            item["recipient_address"] = cells[1] if offset == 0 else ""
            item["recipient_postcode"] = cells[2] if offset == 0 else ""
            item["recipient_location"] = cells[3 + offset]
            item["amount"] = cells[4 + offset].replace(".", "").replace(",", ".")
            item["year"] = self.year
            item["country"] = "IT"
            item["currency"] = "EUR"
            yield item
