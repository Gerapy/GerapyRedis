from scrapy.statscollectors import StatsCollector
from gerapy_redis.connection import from_settings as redis_from_settings
import re
from .picklecompat import dumps, loads


def load(value):
    if not value:
        return None
    if isinstance(value, bytes):
        v = value.decode('utf-8')
        if v.isdigit():
            return int(v)
        if re.match('\d+\.\d+', v):
            return float(v)
    if isinstance(value, bytes):
        value = value.decode('utf-8')
        return loads(value)
    return value


def dump(value):
    if not value:
        return None
    if isinstance(value, (int, float, str)):
        return value
    return dumps(value)


class RedisStatsCollector(StatsCollector):
    """
    Stats Collector based on Redis
    """
    
    def __init__(self, crawler, spider=None):
        super().__init__(crawler)
        self.redis = redis_from_settings(crawler.settings)
        self.spider = spider
    
    @classmethod
    def from_spider(cls, spider):
        return cls(spider.crawler, spider)
    
    def _get_key(self, key, spider=None):
        if spider is None:
            name = '<scrapy>'
        elif self.spider is not None:
            name = self.spider.name
        else:
            name = spider.name
        return '%s:stats:%s' % (name, key)
    
    def get_value(self, key, default=None, spider=None):
        key = self._get_key(key, spider)
        value = self.redis.get(key)
        value = load(value)
        if value is None:
            return default
        else:
            return value
    
    def get_stats(self, spider=None):
        keys = self.redis.keys(self._get_key('*', spider))
        return {k: v for (k, v) in self.redis.mget(*keys)}
    
    def set_value(self, key, value, spider=None):
        key = self._get_key(key, spider)
        value = dump(value)
        self.redis.set(key, value)
    
    def inc_value(self, key, count=1, start=0, spider=None):
        pipe = self.redis.pipeline()
        key = self._get_key(key, spider)
        pipe.setnx(key, start)
        pipe.incrby(key, count)
        pipe.execute()
    
    def max_value(self, key, value, spider=None):
        key = self._get_key(key, spider)
        current = self.get_value(key)
        if current is None or value > current:
            self.set_value(key, value)
    
    def min_value(self, key, value, spider=None):
        key = self._get_key(key, spider)
        current = self.get_value(key)
        print('current', current, value)
        if current is None or value < current:
            self.set_value(key, value)
    
    def clear_stats(self, spider=None):
        keys = self.redis.keys(self._get_key('*', spider))
        self.redis.delete(*keys)
    
    def open_spider(self, spider):
        self.spider = spider
    
    def close_spider(self, spider, reason=None):
        self.spider = None
