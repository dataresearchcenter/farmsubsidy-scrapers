"""A bunch of pipelines."""

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class DropSubsidyFilter(object):
    """Drop unwanted subsidy items."""

    @staticmethod
    def process_item(item, spider):
        args = (spider.name, item["scheme"], item["recipient_name"])

        if spider.name == "croatia":
            if "nacionalna" in item["agency"].lower():
                raise DropItem('%s dropped national subsidy item "%s" for %s' % args)
            else:
                return item

        if spider.name == "lithuania":
            if not item["amount"]:
                raise DropItem('%s dropped empty subsidy item "%s" for %s' % args)
            else:
                return item
        return item


class DuplicatesPipeline:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter["id"] in self.ids_seen:
            raise DropItem(f"Item ID already seen: {adapter['id']}")
        else:
            self.ids_seen.add(adapter["id"])
            # Remove 'id' before returning the item
            del adapter["id"]
            return item
