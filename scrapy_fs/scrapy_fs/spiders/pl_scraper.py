# -*- encoding: utf-8 -*-
import math
import re

import scrapy
from scrapy.spiders import Spider

from ..items import FarmSubsidyItem


class PLSpider(Spider):
    name = "PL"

    custom_settings = {"AUTOTHROTTLE_ENABLED": True, "LOG_LEVEL": "INFO"}

    def __init__(self, year=None):
        self.year = int(year)

    # https://beneficjenciwpr.minrol.gov.pl/api/beneficiary?first=0&page=10000&size=50&sort=&currency.equals=pln&year.equals=2022

    def start_requests(self):
        max_page = 30000
        for x in range(1, max_page):
            url = f"https://beneficjenciwpr.minrol.gov.pl/api/beneficiary?first=0&page={x}&size=50&sort=&currency.equals=pln&year.equals={self.year}"
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):

        # print(response.json())

        # item
        """
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

        for item in response.json():
            # Extract data fields
            name = ""
            first_name = item.get("firstname", "")

            if first_name:
                name = first_name + " "

            if item.get("name", ""):
                name += item.get("name", "")

            locality = ""
            if item.get("state"):
                locality = item.get("state", "")

            if item.get("substate", ""):
                if locality:
                    locality += ", "
                locality += item.get("substate", "")

            amount = item.get("total", 0.0)

            yield FarmSubsidyItem(
                country="PL",
                currency="PLN",
                year=self.year,
                recipient_name=name,
                recipient_location=locality,
                amount=amount,
            )
