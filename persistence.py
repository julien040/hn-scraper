import redis
from os import getenv
from rq import Queue


# The connection to the Redis server for the queue is on database 0.
redis_connection_queue = redis.StrictRedis().from_url(getenv("REDIS_URL_QUEUE"))

# The connection to the Redis server for the post data is on database 1.
rPost = redis.Redis().from_url(getenv("REDIS_URL_POST"))

redis_queue = Queue(connection=redis_connection_queue)
