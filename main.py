from threading import Timer
from time import sleep

from hn_api import add_story_redis
from persistence import rQueue
from rq import Queue

q = Queue(connection=rQueue)

for i in range(3):
    q.enqueue(add_story_redis, "36113430")
    print("Enqueued job {}".format(i))
