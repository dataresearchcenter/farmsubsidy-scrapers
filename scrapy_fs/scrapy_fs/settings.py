# Scrapy settings for scrapy_fs project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = "scrapy_fs"

SPIDER_MODULES = ["scrapy_fs.spiders"]
NEWSPIDER_MODULE = "scrapy_fs.spiders"

# AUTOTHROTTLE_ENABLED = True

# ITEM_PIPELINES = {
#     'scrapy_fs.pipelines.DropSubsidyFilter': 100,
# }

USER_AGENT = "Farm subsidy scraper bot (https://farmsubsidy.org)"

RETRY_TIMES = 5

# For EE
URLLENGTH_LIMIT = 6000

# Enable HTTP caching
HTTPCACHE_ENABLED = True

# Cache storage location
HTTPCACHE_DIR = "cache"

# Cache expiration time (in seconds) - default 0 = never expire
HTTPCACHE_EXPIRATION_SECS = 0

# Cache storage backend
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Optional: Ignore these HTTP codes for caching
HTTPCACHE_IGNORE_HTTP_CODES = [500, 502, 503, 504, 408]
