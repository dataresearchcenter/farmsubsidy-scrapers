"""Item and Loader classes."""

# Farm subsidy data format:
# http://farmsubsidy.readthedocs.org/en/latest/scraper.html#scraper-data-format

from itemloaders.processors import Join, MapCompose, TakeFirst
from scrapy import Field, Item
from scrapy.loader import ItemLoader
from scrapy_fs.scrubbers import (
    filter_croatian_postcode,
    filter_croatian_recipient_id,
    filter_euro_amount,
    filter_lithuanian_location,
    filter_lithuanian_recipient_id,
    make_comma_proof,
    select_after_semicolon,
    strip_line_breaks,
)


class FarmSubsidyItem(Item):
    id = Field()  # Only to identify duplicates with scrapy
    year = Field()
    country = Field()  # Two letters ISO 3166
    recipient_id = Field()
    recipient_name = Field()
    recipient_address = Field()
    recipient_postcode = Field()
    recipient_location = Field()
    recipient_url = Field()
    agency = Field()
    scheme_code = Field()
    scheme_name = Field()
    scheme_description = Field()
    amount = Field()
    currency = Field()


class CroatiaItemLoader(ItemLoader):
    default_output_processor = TakeFirst()

    year_in = MapCompose(int)
    amount_in = MapCompose(filter_euro_amount, float)
    scheme_in = MapCompose(strip_line_breaks)
    recipient_id_in = MapCompose(filter_croatian_recipient_id)
    recipient_name_in = MapCompose(select_after_semicolon, make_comma_proof)
    recipient_location_in = MapCompose(select_after_semicolon)
    recipient_postcode_in = MapCompose(filter_croatian_postcode)


class LithuanianLoader(ItemLoader):
    default_output_processor = TakeFirst()

    year_in = MapCompose(int)
    amount_in = MapCompose(filter_euro_amount, float)
    recipient_id_in = MapCompose(filter_lithuanian_recipient_id, str)

    recipient_location_in = MapCompose(filter_lithuanian_location)
    recipient_location_out = Join(separator=", ")
