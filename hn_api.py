import requests
from time import sleep
from persistence import rPost
from retry import retry
from os import getenv

proxies = {
    'http': getenv("PROXY_URL_USA"),
    'https': getenv("PROXY_URL_USA"),
}

# We define a decorator to retry the function in case of failure.


@retry(tries=10, delay=1)
def fetch_post_hn(id: str) -> dict:
    """
    Fetch a story from Hacker News.

    Returns:
        dict: A story object.
    """
    story_url = 'https://hacker-news.firebaseio.com/v0/item/{}.json'.format(
        id)
    story = requests.get(story_url, proxies=proxies).json()
    if type(story) is not dict:
        raise Exception("Story {} is not a dictionary.".format(id))

    # Check if the story has all the required fields.
    required_fields = ["by", "type"]
    for field in required_fields:
        if field not in story:
            raise Exception("Story {} is missing field {}.".format(id, field))

    return story


def add_story_redis(id: str):
    """
    Fetch an item from Hacker News and add it to Redis.
    If the story is already in Redis, it is updated.
    If not, we compute embeddings too.

    If the item is not a story, we do nothing.

    Args:
        id (str): The ID of the post

    """
    # We fetch the story from the API.
    story = fetch_post_hn(id)

    # We check if the item is a story.
    if story["type"] != "story":
        print("Item {} is not a story.".format(id))
        return

    # We check if the story has all the required fields.
    required_fields = ["by", "title", "score", "time", "descendants"]
    for field in required_fields:
        if field not in story:
            print("Story {} is missing field {}.".format(id, field))
            return

    # We check if the story is already in Redis.
    storyAlreadyInRedis: bool = rPost.exists(id) >= 1

    # We add the story to Redis.
    mapping = {
        "by": story["by"],
        "title": story["title"],
        # We check if the story has a URL. If not, we set it to an empty string.
        "url": story["url"] if "url" in story else "",
        "score": story["score"],
        "time": story["time"],
        "comments": story["descendants"],

    }

    redisPostID = "hn:{}".format(id)

    # We add the story to Redis.
    rPost.hset(redisPostID, mapping=mapping)


def get_max_id_HN() -> int:
    """
    Get the max ID from Hacker News.

    Returns:
        int: The max ID.
    """
    max_id = requests.get(
        'https://hacker-news.firebaseio.com/v0/maxitem.json', proxies=proxies).json()
    # We convert the ID to an integer in case of the API returning a string.
    return int(max_id)
