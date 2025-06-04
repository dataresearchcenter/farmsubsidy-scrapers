"""

source data required columns

    country
    year
    recipient_name
    amount
    currency


additional columns that are taken if present

    recipient_id (helps for deduping if source supplies an identifier)
    recipient_address
    recipient_street
    recipient_street1
    recipient_street2
    recipient_postcode
    recipient_country
    recipient_url (source url to original data platform?)
    scheme (EU measurement)
    scheme_name
    scheme_code
    scheme_code_short
    scheme_description
    scheme_1
    scheme_2
    amount_original
    currency_original

    # print(response.json())
    # https://beneficjenciwpr.minrol.gov.pl/api/beneficiary?first=0&page=10000&size=50&sort=&currency.equals=pln&year.equals=2022

        # item

                {
          "id" : 14139415,
          "identyfikator" : null,
          "year" : 2022,
          "firstname" : "ANDRZEJ",
          "surname" : "NOWOTNIK",
          "name" : null,
          "taxnumber" : "8111007587",
          "idnumber" : "71113001378",
          "otherdocument" : null,
          "regon" : null,
          "state" : "KAZANÓW",
          "postal" : "26-713",
          "payment" : 0.0,
          "efrg" : 0.0,
          "prow" : 0.0,
          "total" : 28384.98,
          "substate" : "ZWOLEŃSKI",
          "substate_postal" : "26-713"
        }, {
"""

import math

import scrapy
from scrapy.spiders import Spider

from ..items import FarmSubsidyItem


class PLSpider(Spider):
    name = "PL"

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": False,
        "LOG_LEVEL": "INFO",
        # "CONCURRENT_REQUESTS": 20,
        # "CONCURRENT_REQUESTS_PER_DOMAIN": 20,
    }

    def __init__(self, year=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.year = int(year)
        self.page_size = 50
        self.expected_total_count = None  # Initialize expected_total_count
        self.item_count = 0  # Initialize item counter

    def start_requests(self):
        url = f"https://beneficjenciwpr.minrol.gov.pl/api/beneficiary?first=0&page=0&size={self.page_size}&sort=&currency.equals=pln&year.equals={self.year}"
        yield scrapy.Request(url, callback=self.parse_total_count)

    def parse_total_count(self, response):
        total_count = response.headers.get("X-Total-Count", None)
        if not total_count:
            raise ValueError(
                f"X-Total-Count header not found in the response for year {self.year}"
            )
        self.expected_total_count = int(total_count)
        self.item_count = 0
        max_page = math.ceil(self.expected_total_count / self.page_size)
        print(f"Total pages: {max_page}")
        for x in range(0, max_page + 1):
            url = f"https://beneficjenciwpr.minrol.gov.pl/api/beneficiary?first=0&page={x}&size={self.page_size}&sort=&currency.equals=pln&year.equals={self.year}"
            yield scrapy.Request(url, callback=self.parse)

    # GET https://beneficjenciwpr.minrol.gov.pl/api/beneficiary/13139415

    def parse(self, response):
        for item in response.json():
            # Prepare partial data
            name = ""
            first_name = item.get("firstname", "")
            if first_name:
                name = first_name + " "
            if item.get("name", ""):
                name += item.get("name", "")
            locality = item.get("state", "")
            recipient_postcode = item.get("postal", "")
            substate = item.get("substate", "")
            if substate and substate != item.get("state", ""):
                if locality:
                    locality += ", "
                    locality += substate
                else:
                    locality = substate
            substate_postal = item.get("substate_postal", "")

            if substate_postal and substate_postal != recipient_postcode:
                if locality:
                    locality += ", "
                    locality += substate_postal
                else:
                    locality = substate_postal

            # amount = item.get("total", 0.0)
            identifier = f"{item.get('id', '')}-{item.get('taxnumber', '')}-{item.get('idnumber', '')}"

            # Prepare meta for detail request
            meta = {
                "country": "PL",
                "currency": "PLN",
                "year": self.year,
                "recipient_name": name,
                "recipient_location": locality,
                "recipient_postcode": recipient_postcode,
                # "amount": amount,
                "recipient_id": identifier,
            }
            detail_url = f"https://beneficjenciwpr.minrol.gov.pl/api/beneficiary/{item.get('id')}"
            yield scrapy.Request(detail_url, callback=self.parse_detail, meta=meta)

    def parse_detail(self, response):
        # Extract additional details from detail page
        meta = response.meta
        detail_data = response.json()

        # Only keep allowed fields
        allowed_fields = {
            "country",
            "currency",
            "year",
            "recipient_name",
            "recipient_location",
            "recipient_postcode",
            "recipient_id",
        }
        filtered_meta = {k: v for k, v in meta.items() if k in allowed_fields}

        for key, value in detail_data.items():
            if isinstance(value, (int, float)) and value != 0:
                if key in ["year"]:
                    continue
                # print(f"Key: {key}, Value: {value}")
                yield FarmSubsidyItem(**filtered_meta, scheme=key, amount=value)

        self.item_count += 1

    def closed(self, reason):
        # For 2022, 50 are missing. Unclear why.
        if hasattr(self, "expected_total_count"):
            print(
                f"Expected items: {self.expected_total_count}, Scraped items: {self.item_count}, Difference: {self.item_count - self.expected_total_count}"
            )
            if self.item_count != self.expected_total_count:
                print("WARNING: Scraped item count does not match expected total!")
