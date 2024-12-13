import re

import scrapy
from scrapy.spiders import Spider

from ..items import FarmSubsidyItem

# Acutally faster to scrape sequentially and not in parallel
SEQUENTIAL = True


class ROSpider(Spider):
    """
    Spider for Romanias's CAP payments
    """

    name = "RO"

    custom_settings = {"AUTOTHROTTLE_ENABLED": not SEQUENTIAL, "LOG_LEVEL": "INFO"}

    # AMOUNT_RE = re.compile('[^\d\.]')
    # BAD_NAME = u'ID not available'

    def __init__(self, year=None):
        self.year = int(year)
        # self.start_urls = [
        #     f"https://plati.afir.info/Plati/AfisareListaPlatiEN?pageNumber={x}&anFinanciar={self.year}"
        #     for x in range(1, 1000)
        # ]

    def start_requests(self):
        url = f"https://plati.afir.info/Plati/AfisareListaPlatiEN?pageNumber=1&anFinanciar={self.year}"
        if SEQUENTIAL:
            yield scrapy.Request(url, callback=self.parse)
        else:
            yield scrapy.Request(url, callback=self.parse_total_pages)

    def parse_total_pages(self, response):
        total_pages_url = response.css("a#btn-last-page::attr(href)").get()

        if total_pages_url:
            page_pattern = re.compile(r"pageNumber=(\d+)")
            match = page_pattern.search(total_pages_url)

            if match:
                total_pages = int(match.group(1))
                for x in range(1, total_pages + 1):
                    url = f"https://plati.afir.info/Plati/AfisareListaPlatiEN?pageNumber={x}&anFinanciar={self.year}"
                    yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        # Loop through each table row
        rows = response.css("#content tr")

        for row in rows:
            # Extract data fields
            beneficiary_name = row.xpath(
                ".//td[contains(., 'BENEFICIARY NAME')]/div[@class='fw-bold-content']/text()"
            ).get()
            beneficiary_last_name = row.xpath(
                """.//td[contains(., "BENEFICIARY'S LAST NAME")]/div[@class='fw-bold-content']/text()"""
            ).get()
            beneficiary_parent_company = row.xpath(
                ".//td[contains(., 'PARENT COMPANY NAME AND TAX REGISTRATION CODE')]/div[@class='fw-bold-content']/text()"
            ).get()
            locality = row.xpath(
                ".//td[contains(., 'LOCALITY')]/div[@class='fw-bold-content']/text()"
            ).get()
            measure_code = row.xpath(
                ".//td[normalize-space(.//div[@class='fw-bold'])='MEASURE/INTERVENTION TYPE CODE']/div[@class='fw-bold-content']/text()"
            ).get()

            objective = row.xpath(
                ".//td[contains(., 'OBJECTIVE')]/div[@class='fw-bold-content']/text()"
            ).get()

            fega_operation_amount = row.xpath(
                ".//td[contains(., 'FEGA OPERATION AMOUNT')]/div[@class='fw-bold-content']/text()"
            ).get()
            feadr_operation_amount = row.xpath(
                ".//td[contains(., 'FEADR OPERATION AMOUNT')]/div[@class='fw-bold-content']/text()"
            ).get()
            total_feadr_amount = row.xpath(
                ".//td[contains(., 'TOTAL FEADR AMOUNT')]/div[@class='fw-bold-content']/text()"
            ).get()
            operation_related_amount = row.xpath(
                ".//td[contains(., 'OPERATION-RELATED AMOUNT')]/div[@class='fw-bold-content']/text()"
            ).get()
            total_bene_cof_amount = row.xpath(
                ".//td[contains(., 'TOTAL BENEFICIARY COFINANCING AMOUNT')]/div[@class='fw-bold-content']/text()"
            ).get()
            total_eu_amount = row.xpath(
                ".//td[contains(., 'TOTAL EU AMOUNT FOR BENEFICIARY')]/div[@class='fw-bold-content']/text()"
            ).get()

            name = beneficiary_name.strip() if beneficiary_name else "N.N."

            if beneficiary_last_name:
                name += " " + beneficiary_last_name.strip()

            if beneficiary_parent_company:
                name += ", " + beneficiary_parent_company.strip()

            schema = (
                measure_code
                if measure_code
                else "" + (" - " + objective if objective else "")
            )

            yield FarmSubsidyItem(
                country="RO",
                currency="RON",
                year=self.year,
                recipient_name=name,
                recipient_location=locality,
                scheme=schema,
                amount=total_eu_amount,
            )

        if SEQUENTIAL:
            # Find the link to the next page
            next_page = response.css("a#btn-next-page::attr(href)").get()

            # If there's a next page, yield a new request
            if next_page:
                next_page_url = response.urljoin(next_page)  # Make the URL absolute
                yield scrapy.Request(url=next_page_url, callback=self.parse)
