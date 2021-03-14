from scrapy.statscollectors import StatsCollector
from gerapy_redis.connection import from_settings as redis_from_settings


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
        if value is None:
            return default
        else:
            return value
    
    def get_stats(self, spider=None):
        keys = self.redis.keys(self._get_key('*', spider))
        return {k: v for (k, v) in self.redis.mget(*keys)}
    
    def set_value(self, key, value, spider=None):
        key = self._get_key(key, spider)
        self.redis.set(key, value)
    
    def inc_value(self, key, count=1, start=0, spider=None):
        pipe = self.redis.pipeline()
        key = self._get_key(key, spider)
        pipe.setnx(key, start)
        pipe.incrby(key, count)
        pipe.execute()
    
    def max_value(self, key, value, spider=None):
        key = self._get_key(key, spider)
        pipe = self.redis.pipeline()
        pipe.zadd(key, value, value)
        pipe.zremrangebyrank(key, 0, -2)
        pipe.execute()
    
    def min_value(self, key, value, spider=None):
        key = self._get_key(key, spider)
        pipe = self.redis.pipeline()
        pipe.zadd(key, value, value)
        pipe.zremrangebyrank(key, 2, -1)
        pipe.execute()
    
    def clear_stats(self, spider=None):
        keys = self.redis.keys(self._get_key('*', spider))
        self.redis.delete(*keys)
    
    def open_spider(self, spider):
        self.spider = spider
    
    def close_spider(self, spider, reason=None):
        self.spider = None
