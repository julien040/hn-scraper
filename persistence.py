import redis
from os import getenv

# The connection to the Redis server for the queue is on database 0.
rQueue = redis.Redis(host='localhost', port=6379, db=0, username='default',
                     password=getenv('REDIS_PASSWORD'))

# The connection to the Redis server for the post data is on database 1.
rPost = redis.Redis(host='localhost', port=6379, db=1, username='default',
                    password=getenv('REDIS_PASSWORD'))
