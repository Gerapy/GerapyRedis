# Gerapy Redis

This is a package for supporting distribution in Scrapy using Redis, also this
package is a module in [Gerapy](https://github.com/Gerapy/Gerapy).

This package is almost copied from [https://github.com/rmax/scrapy-redis](https://github.com/rmax/scrapy-redis).

## Change

Removed RedisSpider, move the logic to Scheduler. It will pre enqueue 
all start requests to Redis Queue instead of adding one start request
when crawler is idle.

Arg: `SCHEDULER_PRE_ENQUEUE_ALL_START_REQUESTS`, default to `True`.

## Installation

```shell script
pip3 install gerapy-redis
```

## Usage

```python
# Enables scheduling storing requests queue in redis.
SCHEDULER = "gerapy_redis.scheduler.Scheduler"

# Ensure all spiders share same duplicates filter through redis.
DUPEFILTER_CLASS = "gerapy_redis.dupefilter.RFPDupeFilter"

# Default requests serializer is pickle, but it can be changed to any module
# with loads and dumps functions. Note that pickle is not compatible between
# python versions.
# Caveat: In python 3.x, the serializer must return strings keys and support
# bytes as values. Because of this reason the json or msgpack module will not
# work by default. In python 2.x there is no such issue and you can use
# 'json' or 'msgpack' as serializers.
#SCHEDULER_SERIALIZER = "gerapy_redis.picklecompat"

# Don't cleanup redis queues, allows to pause/resume crawls.
#SCHEDULER_PERSIST = True

# Pre enqueue all start requests to queue, (default True)
#SCHEDULER_PRE_ENQUEUE_ALL_START_REQUESTS = True

# Schedule requests using a priority queue. (default)
#SCHEDULER_QUEUE_CLASS = 'gerapy_redis.queue.PriorityQueue'

# Alternative queues.
#SCHEDULER_QUEUE_CLASS = 'gerapy_redis.queue.FifoQueue'
#SCHEDULER_QUEUE_CLASS = 'gerapy_redis.queue.LifoQueue'

# Max idle time to prevent the spider from being closed when distributed crawling.
# This only works if queue class is SpiderQueue or SpiderStack,
# and may also block the same time when your spider start at the first time (because the queue is empty).
#SCHEDULER_IDLE_BEFORE_CLOSE = 10

# Store scraped item in redis for post-processing.
ITEM_PIPELINES = {
    'gerapy_redis.pipelines.RedisPipeline': 300
}

# The item pipeline serializes and stores the items in this redis key.
#REDIS_ITEMS_KEY = '%(spider)s:items'

# The items serializer is by default ScrapyJSONEncoder. You can use any
# importable path to a callable object.
#REDIS_ITEMS_SERIALIZER = 'json.dumps'

# Specify the host and port to use when connecting to Redis (optional).
#REDIS_HOST = 'localhost'
#REDIS_PORT = 6379

# Specify the full Redis URL for connecting (optional).
# If set, this takes precedence over the REDIS_HOST and REDIS_PORT settings.
#REDIS_URL = 'redis://user:pass@hostname:9001'

# Custom redis client parameters (i.e.: socket timeout, etc.)
#REDIS_PARAMS  = {}
# Use custom redis client class.
#REDIS_PARAMS['redis_cls'] = 'myproject.RedisClient'

# If True, it uses redis' ``SPOP`` operation. You have to use the ``SADD``
# command to add URLs to the redis queue. This could be useful if you
# want to avoid duplicates in your start urls list and the order of
# processing does not matter.
#REDIS_START_URLS_AS_SET = False

# If True, it uses redis ``zrevrange`` and ``zremrangebyrank`` operation. You have to use the ``zadd``
# command to add URLS and Scores to redis queue. This could be useful if you
# want to use priority and avoid duplicates in your start urls list.
#REDIS_START_URLS_AS_ZSET = False

# Default start urls key for RedisSpider and RedisCrawlSpider.
#REDIS_START_URLS_KEY = '%(name)s:start_urls'

# Use other encoding than utf-8 for redis.
#REDIS_ENCODING = 'latin1'
```

For more information, please refer to [https://github.com/rmax/scrapy-redis](https://github.com/rmax/scrapy-redis).