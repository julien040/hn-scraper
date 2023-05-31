from threading import Timer
from persistence import redis_connection_queue, redis_queue
from hn_api import get_max_id_HN, add_story_redis


def pollNewStory():
    print("Polling new stories.")
    """
    Poll the Hacker News API to get the max ID.
    Then, we add the new ID to the queue.

    To do so, we compare the max ID from redis. If it's different, we add all the IDs in the interval
    because HN is sequential.
    """

    # We get the max ID from the API.
    maxID = get_max_id_HN()

    # We get the max ID from Redis.
    maxIDRedis = redis_connection_queue.get("max:ID:hn")

    # We convert the max ID from Redis to an integer or set it to 0 if it is None.
    maxIDRedis = int(maxIDRedis) if maxIDRedis is not None else 0

    # We check if the max ID from the API is greater than the max ID from Redis.
    if maxID > maxIDRedis:
        print("New stories available. Adding them to the queue.")
        print("{} new stories ({} - {}).".format(maxID -
              maxIDRedis, maxIDRedis + 1, maxID))
        # We add all the IDs in the interval.
        for id in range(maxIDRedis + 1, maxID + 1):
            # We add the ID to the queue.
            redis_queue.enqueue(add_story_redis, str(id))

        # We update the max ID in Redis.
        redis_connection_queue.set("max:ID:hn", maxID)

    # We schedule the next poll.
    Timer(10, pollNewStory).start()
