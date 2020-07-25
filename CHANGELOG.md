# Gerapy Redis Changelog

## 0.0.1 (2020-07-25)

### Features

* Removed RedisSpider, move the logic to Scheduler. It will pre enqueue 
all start requests to Redis Queue instead of adding one start request
when crawler is idle.
