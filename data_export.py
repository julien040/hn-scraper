from persistence import rPost, redis_queue, redis_connection_queue
from os import remove, mkdir
from bz2 import decompress
from json import loads, dumps
from embeddings import add_embeddings_redis
import duckdb
import time
import csv


SCAN_COUNT = 500000  # The number of posts to scan in Redis per iteration.
PATTERN = "hn:*"  # The pattern to use to scan Redis.
DATABASE_NAME = "hn.db"  # The name of the database to use.

# Delete the database if it already exists.
try:
    remove(DATABASE_NAME)
except OSError:
    print("No database to delete.")
    pass

# Open the database.
con = duckdb.connect(database=DATABASE_NAME, read_only=False)

# Create the table.
con.execute("""CREATE TABLE story (id INTEGER PRIMARY KEY, title VARCHAR, url VARCHAR, score INTEGER, 
time INTEGER, comments INTEGER, author VARCHAR, embeddings FLOAT[]);""")


def poll_post_duckDB(cursor: int = 0):
    """
    Poll the posts in Redis and export them to a DuckDB database.

    To do so, we iterate over the posts in Redis using the SCAN command.
    """

    before = time.time()
    res_scan = rPost.scan(cursor=cursor, count=SCAN_COUNT)
    print("Polling posts for embeddings. Cursor: {}. New cursor: {}".format(
        cursor, res_scan[0]))
    print("{} posts to scan.".format(len(res_scan[1])))
    print("Scanning took {} ms".format((time.time() - before) * 1000))

    # Ids are prefixed with "hn:". We remove it.
    id = [postID.decode("utf-8")[3:] for postID in res_scan[1]]

    # We iterate over the posts to get their fields
    # and add them to the database.

    before = time.time()
    pipe = rPost.pipeline()
    for postID in res_scan[1]:
        pipe.hmget(postID, "title", "url", "score",
                   "time", "comments", "by", "embeddings")

    # We execute the pipeline.
    res = pipe.execute()
    print("Fetching took {} ms".format((time.time() - before) * 1000))

    # A list of posts to add to the database.
    posts = []

    for i in range(len(res)):
        embedding: list[float] = None
        if res[i][6] != None:
            # We decompress the embedding and parse the JSON.
            embedding = loads(decompress(res[i][6]))

        posts.append({
            "id": id[i],
            "title": res[i][0].decode("utf-8"),
            "url": res[i][1].decode("utf-8"),
            "score": int(res[i][2]),
            "time": int(res[i][3]),
            "comments": int(res[i][4]),
            "author": res[i][5].decode("utf-8"),
            "embeddings": embedding
        })
    # Inserting using SQL is too slow. We use the CSV import instead.
    # We dump the posts to a temp csv file.

    cursor = con.cursor()

    before = time.time()
    with open("temp.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(posts[0].keys())
        for post in posts:
            writer.writerow(post.values())

    # We import the CSV file.
    cursor.execute(
        "COPY story FROM 'temp.csv' ( DELIMITER ',', HEADER TRUE );")
    print("Inserting took {} ms \n".format((time.time() - before) * 1000))

    # We check if we need to continue scanning.
    if res_scan[0] != 0:
        poll_post_duckDB(cursor=res_scan[0])


def export_various_format():
    """
    Export the DuckDB database to various formats.
    """
    # Check if folder data_export exists.
    try:
        mkdir("data_export")
    except OSError:
        pass

    # Export to CSV.
    print("Exporting to CSV.")
    con.execute(
        "COPY (SELECT id, title, url, score, time, comments, author FROM story) TO 'data_export/story.csv' ( DELIMITER ',', HEADER);")
    con.execute("COPY (SELECT * FROM story WHERE len(embeddings) > 0) TO 'data_export/story_with_embeddings.csv' ( DELIMITER ',', HEADER);")

    # Export to JSON.
    print("Exporting to JSON.")
    con.execute(
        "COPY (SELECT id, title, url, score, time, comments, author FROM story) TO 'data_export/story.json' ( FORMAT JSON, ARRAY TRUE );")
    con.execute("COPY (SELECT * FROM story WHERE len(embeddings) > 0) TO 'data_export/story_with_embeddings.json' ( FORMAT JSON, ARRAY TRUE );")

    # Export to Parquet.
    print("Exporting to Parquet.")
    con.execute("COPY (SELECT id, title, url, score, time, comments, author FROM story) TO 'data_export/story.parquet' ( FORMAT PARQUET );")
    con.execute("COPY (SELECT * FROM story WHERE len(embeddings) > 0) TO 'data_export/story_with_embeddings.parquet' ( FORMAT PARQUET );")


if __name__ == "__main__":
    # We start the polling.
    now = time.time()
    poll_post_duckDB()

    # We export the database to various formats.
    export_various_format()

    # We delete the temp file.
    try:
        remove("temp.csv")
    except OSError:
        pass

    print("Polling total took {} ms".format((time.time() - now) * 1000))
