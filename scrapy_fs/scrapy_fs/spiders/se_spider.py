import re

import scrapy
from scrapy.spiders import Spider

from ..items import FarmSubsidyItem


class SESpider(Spider):
    """
    Swedens CAP payments
    """

    name = "SE"

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": False,
        "LOG_LEVEL": "INFO",
        "HTTPCACHE_ENABLED": True,
    }

    # AMOUNT_RE = re.compile('[^\d\.]')
    # BAD_NAME = u'ID not available'

    def __init__(self, year=None):
        self.year = int(year)

    """
    AJAXREQUEST=j_id0
searchResultsForm_SUBMIT=1
javax.faces.ViewState=+cNxfEoERH6+x0Xj4WkRQvRibhZzngTTR/x6Hdk1mPOinYw2nGzwLsiAM2oJK5RpGyR9BS/c+2rs++XqtfkHttPhx4i4JCKa3l9BCKNBpLO57+iGQmnqNyr+ktlaah24qq2mBMBsvN20b37y
searchResultsForm:j_id_jsp_121545192_121=2
ajaxSingle=searchResultsForm:j_id_jsp_121545192_121
AJAX:EVENTS_COUNT=1
    """

    def start_requests(self):
        for x in range(1, 620):
            yield scrapy.FormRequest(
                url="https://etjanst.sjv.se/asken/faces/jbstod/searchJbstod.jsp",
                formdata={
                    "AJAXREQUEST": "j_id0",
                    "searchResultsForm_SUBMIT": "1",
                    "javax.faces.ViewState": "+cNxfEoERH6+x0Xj4WkRQvRibhZzngTTR/x6Hdk1mPOinYw2nGzwLhk8iJUV9M9MwyQ3m0dr+rHkpwRZpSqEIU//L6sHgV8mVipq9MYy9NzRcflfhla1xVtiQv8nYAb+z2VHYdtMx/yFoQbO",
                    "searchResultsForm:j_id_jsp_121545192_121": str(x),
                    "ajaxSingle": "searchResultsForm:j_id_jsp_121545192_121",
                    "AJAX:EVENTS_COUNT": "1",
                },
                cookies={
                    "BIGipServerpool_epublik_http_8106": "2185762220.43551.0000",
                    "JSESSIONID": "4921943166FA5C8AD0CE5869AE19169F",
                },
                callback=self.parse,
            )
        # yield scrapy.Request(url, callback=self.parse)

    # def parse_total_pages(self, response):
    #     total_pages_url = response.css("a#btn-last-page::attr(href)").get()

    #     if total_pages_url:
    #         page_pattern = re.compile(r"pageNumber=(\d+)")
    #         match = page_pattern.search(total_pages_url)

    #         if match:
    #             total_pages = int(match.group(1))
    #             for x in range(1, total_pages + 1):
    #                 url = f"https://plati.afir.info/Plati/AfisareListaPlatiEN?pageNumber={x}&anFinanciar={self.year}"
    #                 yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        print(response)
        # place = response.css("div.kommun")
        print(response.text)

        # Loop through each table row
        rows = response.css("#content tr")

        # for row in rows:
        #     # Extract data fields
        #     beneficiary_name = row.xpath(
        #         ".//td[contains(., 'BENEFICIARY NAME')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     beneficiary_last_name = row.xpath(
        #         """.//td[contains(., "BENEFICIARY'S LAST NAME")]/div[@class='fw-bold-content']/text()"""
        #     ).get()
        #     beneficiary_parent_company = row.xpath(
        #         ".//td[contains(., 'PARENT COMPANY NAME AND TAX REGISTRATION CODE')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     locality = row.xpath(
        #         ".//td[contains(., 'LOCALITY')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     measure_code = row.xpath(
        #         ".//td[normalize-space(.//div[@class='fw-bold'])='MEASURE/INTERVENTION TYPE CODE']/div[@class='fw-bold-content']/text()"
        #     ).get()

        #     objective = row.xpath(
        #         ".//td[contains(., 'OBJECTIVE')]/div[@class='fw-bold-content']/text()"
        #     ).get()

        #     fega_operation_amount = row.xpath(
        #         ".//td[contains(., 'FEGA OPERATION AMOUNT')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     feadr_operation_amount = row.xpath(
        #         ".//td[contains(., 'FEADR OPERATION AMOUNT')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     total_feadr_amount = row.xpath(
        #         ".//td[contains(., 'TOTAL FEADR AMOUNT')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     operation_related_amount = row.xpath(
        #         ".//td[contains(., 'OPERATION-RELATED AMOUNT')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     total_bene_cof_amount = row.xpath(
        #         ".//td[contains(., 'TOTAL BENEFICIARY COFINANCING AMOUNT')]/div[@class='fw-bold-content']/text()"
        #     ).get()
        #     total_eu_amount = row.xpath(
        #         ".//td[contains(., 'TOTAL EU AMOUNT FOR BENEFICIARY')]/div[@class='fw-bold-content']/text()"
        #     ).get()

        #     name = beneficiary_name.strip() if beneficiary_name else "N.N."

        #     if beneficiary_last_name:
        #         name += " " + beneficiary_last_name.strip()

        #     if beneficiary_parent_company:
        #         name += ", " + beneficiary_parent_company.strip()

        #     schema = (
        #         measure_code
        #         if measure_code
        #         else "" + (" - " + objective if objective else "")
        #     )

        yield FarmSubsidyItem(
            country="SE",
            currency="SEK",
            year=self.year,
            recipient_name="x",
            recipient_location=place,
            scheme="x",
            amount="x",
        )

    # if SEQUENTIAL:
    #     # Find the link to the next page
    #     next_page = response.css("a#btn-next-page::attr(href)").get()

    #     # If there's a next page, yield a new request
    #     if next_page:
    #         next_page_url = response.urljoin(next_page)  # Make the URL absolute
    #         yield scrapy.Request(url=next_page_url, callback=self.parse)
