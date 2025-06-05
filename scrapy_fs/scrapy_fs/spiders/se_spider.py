import html
from pathlib import Path

import scrapy
from scrapy.spiders import Spider

from ..items import FarmSubsidyItem

viewstate = "+G9PnIYGASYwXFmoQlmp2EGyOtj8B2PaHtFMA7bwNCczxi5T+p6R1a8ClN8qOJLOW0CKmPgpW8hSG2dyU5UNyrvRJuE+jCSzm8Rbc4vMx0yBWtrd4QNhDzI8Nb7jwPvP3i5hcBEl2B4mJebK"
cookie = "F5406B8932E22DB0158986BDEEAEAF09"


class SESpider(Spider):
    """
    Sweden's CAP payments
    """

    name = "SE"

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": False,
        "LOG_LEVEL": "INFO",
        "HTTPCACHE_ENABLED": False,
        "HTTPCACHE_DIR": "httpcache",
        "HTTPCACHE_POLICY": "scrapy_fs.cache_policies.NoContentLengthErrorCachePolicy",
        "CONCURRENT_REQUESTS": 10,
    }

    def __init__(self, year=None):
        super().__init__()
        self.year = int(year)

    def start_requests(self):
        page_size = 100
        for x in range(1, 620):
            page_start = (x - 1) * page_size
            page_end = page_start + page_size - 1

            # check if page_end was saved to file
            # file_name = f"responses/{x}_{page_start}.html"
            # try:
            #     with open(file_name, "rb") as f:
            #         print(f"File {file_name} already exists, skipping request")
            #         continue
            # except FileNotFoundError:
            #     print(f"File {file_name} does not exist, making request")

            yield scrapy.FormRequest(
                url="https://etjanst.sjv.se/asken/faces/jbstod/searchJbstod.jsp",
                formdata={
                    "AJAXREQUEST": "j_id0",
                    "searchResultsForm_SUBMIT": "1",
                    "javax.faces.ViewState": viewstate,
                    "searchResultsForm:j_id_jsp_121545192_121": str(x),
                    "ajaxSingle": "searchResultsForm:j_id_jsp_121545192_121",
                    "AJAX:EVENTS_COUNT": "1",
                },
                cookies={
                    "BIGipServerpool_epublik_http_8106": "2185762220.43551.0000",
                    "JSESSIONID": cookie,
                },
                callback=self.parse,
            )

            for y in range(page_start, page_end + 1):
                key = (
                    "searchResultsForm:foundStodmottagare:"
                    + str(y)
                    + ":j_id_jsp_121545192_98"
                )
                # check if file already exists
                file_path = f"responses/{x}_{y}.html"
                if Path(file_path).exists():
                    text = Path(file_path).read_text(encoding="utf-8")
                    if 'class="stodmottagare selected"' in text:
                        print(
                            f"File {file_path} already exists and contains .selected, skipping request"
                        )
                        absolute_file_path = Path(file_path).resolve()
                        url = "file:///" + str(absolute_file_path)
                        print(f"Using cached file: {url}")
                        yield scrapy.Request(
                            url=url,
                            callback=self.parse_response,
                            encoding="utf-8",
                            headers={
                                "encoding": "utf-8",
                            },
                            meta={"fn": absolute_file_path},
                        )
                        continue
                    else:
                        print(
                            f"File {file_path} exists but does not contain .selected, making request"
                        )

                print(key)
                yield scrapy.FormRequest(
                    dont_filter=True,
                    method="POST",
                    url="https://etjanst.sjv.se/asken/faces/jbstod/searchJbstod.jsp",
                    formdata={
                        "AJAXREQUEST": "j_id0",
                        "searchResultsForm_SUBMIT": "1",
                        "javax.faces.ViewState": viewstate,
                        key: key,
                    },
                    cookies={
                        "BIGipServerpool_epublik_http_8106": "2185762220.43551.0000",
                        "JSESSIONID": cookie,
                    },
                    headers={
                        "Host": "etjanst.sjv.se",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0",
                        "Accept": "*/*",
                        "Accept-Language": "en-US,en;q=0.7,de-DE;q=0.3",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        # "Content-Length": "358",
                        "Origin": "https://etjanst.sjv.se",
                        "DNT": "1",
                        "Sec-GPC": "1",
                        # "Connection": "keep-alive",
                        "Referer": "https://etjanst.sjv.se/asken/faces/jbstod/searchJbstod.jsp",
                    },
                    callback=self.parse2,
                    meta={
                        "page_number": x,
                        "offset": y,
                    },
                )

    def parse2(self, response):
        # get page number and offset from request meta
        page_number = response.meta.get("page_number", None)
        offset = response.meta.get("offset", None)
        file_path = f"responses/{page_number}_{offset}.html"

        with open(file_path, "wb") as f:
            f.write(response.body)

        return self.parse_response(response)

    def parse_response(self, response):
        # print(response.encoding)
        # Read again since the encoding is broken
        body_text = Path(response.meta["fn"]).read_text(encoding="utf-8")

        # Handle XML declaration and namespace issues
        # Remove XML declaration and convert to regular HTML for easier parsing
        if body_text.startswith("<?xml"):
            # Find the end of the XML declaration
            xml_end = body_text.find("?>")
            if xml_end != -1:
                body_text = body_text[xml_end + 2 :].strip()

        # Remove XHTML namespace to make CSS selectors work better
        body_text = body_text.replace('xmlns="http://www.w3.org/1999/xhtml"', "")

        # print(body_text)  # Print first 1000 characters for debugging

        # Create a new response object with cleaned HTML
        from scrapy.http import TextResponse

        clean_response = TextResponse(
            url=response.url,
            body=body_text,
            encoding=response.encoding,  # Pass the original response's encoding
        )

        # Select div.selected and extract farm subsidy data
        selected_div = clean_response.css("div.selected")

        if selected_div:
            # Extract recipient information from the main columns
            name = selected_div.css("div.stodmottagareColumn.namn::text").get()
            county = selected_div.css("div.stodmottagareColumn.lan::text").get()

            municipality = selected_div.css(
                "div.stodmottagareColumn.kommun::text"
            ).get()
            postal_address = selected_div.css(
                "div.stodmottagareColumn.postadress::text"
            ).get()

            # Get total amount
            total_amount_text = selected_div.css(
                "div.stodmottagareColumn.belopp::text"
            ).get()

            if name and total_amount_text:
                # Decode HTML entities and clean up amount
                total_amount_text = html.unescape(total_amount_text).strip()
                # Clean up the recipient name
                name_clean = name.strip()
                # total_amount = total_amount_text.replace("\u00a0", "").replace(" ", "")

                # Construct location string
                location_parts = []
                if municipality:
                    municipality_clean = municipality.strip()
                    location_parts.append(municipality_clean)
                if county:
                    county_clean = county.strip()
                    location_parts.append(county_clean)
                if postal_address:
                    postal_address_clean = postal_address.strip()
                    location_parts.append(postal_address_clean)

                location = ", ".join(location_parts) if location_parts else ""

                # Extract individual subsidy details from stodRow elements
                subsidy_rows = selected_div.css("div.stodRow")

                for row in subsidy_rows:
                    fund_type = row.css("div.stodItem.fondtyp::text").get()
                    category = row.css("div.stodItem.kategori::text").get()
                    specific_goal = row.css("div.stodItem.specifiktmal::text").get()
                    amount_text = row.css("div.stodItem.belopp::text").get()

                    if amount_text:
                        # Clean up the individual amount
                        amount_text = html.unescape(amount_text).strip()
                        amount = amount_text.replace("\u00a0", "").replace(" ", "")

                        scheme_description = ""
                        if fund_type:
                            scheme_description = fund_type.strip()

                        scheme_name = ""
                        scheme_code = ""

                        if category:
                            category_clean = category.strip()
                            # Extract scheme code from category (e.g., "II.1" from "II.1, Basic payment scheme...")
                            if "," in category_clean:
                                scheme_code = category_clean.split(",")[0].strip()
                                scheme_name = category_clean.split(",", 1)[1].strip()
                            else:
                                scheme_code = category_clean
                                scheme_name = category_clean

                        if specific_goal and specific_goal.strip() != "-":
                            scheme_description += (
                                f" | Specific Goal: {specific_goal.strip()}"
                            )

                        yield FarmSubsidyItem(
                            country="SE",
                            currency="SEK",
                            year=self.year,
                            recipient_name=name_clean,
                            recipient_location=location,
                            amount=amount,
                            scheme_name=scheme_name,
                            scheme_code=scheme_code,
                            scheme_description=scheme_description.strip(),
                        )

                # Also yield the total amount as a summary item
                # yield FarmSubsidyItem(
                #     country="SE",
                #     currency="SEK",
                #     year=self.year,
                #     recipient_name=name.strip(),
                #     recipient_location=location,
                #     amount=total_amount,
                #     scheme="Total",
                # )

    def parse(self, _response):
        return
