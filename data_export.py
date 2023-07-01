from persistence import rPost
from os import remove, mkdir, getenv
from bz2 import decompress
from json import loads
from gc import collect
import duckdb
import time
import csv

"""
Some explanation about the data export.

I'm building a giant CSV file with all the posts in it because it's faster than
using INSERT statements. I'm using the COPY command to import the CSV file into
duckDB.

At first, I was using small CSV files (500 000 rows) and importing them into duckDB
using the COPY command. But DuckDB has a weird bug making a segfault. It was hard to debug
because Python would not printed out and would exit silently.
Same with small JSON files.

WARNING:  The program can use quite a lot of memory (more than 16gb). Make sure you have some swap space.
To try to reduce the memory usage, I'm deleting the posts from memory after inserting them into the database.
"""


def poll_post_CSV(cursor: int = 0):
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

    # cursor = con.cursor()

    # Append the posts to the CSV file.
    before = time.time()
    with open("data_export/story.csv", "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=posts[0].keys())
        writer.writerows(posts)

    # We import the CSV file.
    # cursor.execute(
    #    "COPY story FROM 'temp.csv' ( DELIMITER ',', HEADER TRUE );")
    print("Inserting into CSV took {} ms \n".format(
        (time.time() - before) * 1000))

    # Delete from memory the posts.
    print("Cleaning up memory.")
    del posts
    del res
    collect()

    # We check if we need to continue scanning.
    return res_scan[0]


def export_various_format():
    """
    Export the DuckDB database to various formats.
    """

    # Export to CSV.
    print("Exporting to CSV.")
    # We don't need to export the story because we already have it.
    # con.execute(
    #    "COPY (SELECT id, title, url, score, time, comments, author FROM story) TO 'data_export/story.csv' ( DELIMITER ',', HEADER);")
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
    print("Starting data export.")

    print("Trying to create the folder data_export.")
    # Check if folder data_export exists.
    try:
        mkdir("data_export")
    except OSError:
        pass

    # The number of posts to scan in Redis per iteration.
    SCAN_COUNT = int(getenv("EXPORTER_SCAN_COUNT"))
    PATTERN = "hn:*"  # The pattern to use to scan Redis.
    DATABASE_NAME = "data_export/hn.duckdb"  # The name of the database.

    # Delete the database if it already exists.
    try:
        remove(DATABASE_NAME)
    except OSError:
        print("No database to delete.")
        pass

    # Delete the CSV file if it already exists.
    try:
        remove("data_export/story.csv")
    except OSError:
        print("No CSV file to delete.")
        pass

    # Open the database.
    con = duckdb.connect(database=DATABASE_NAME, read_only=False)

    # Create the table.
    con.execute("""CREATE TABLE story (id INTEGER PRIMARY KEY, title VARCHAR, url VARCHAR, score INTEGER,
    time INTEGER, comments INTEGER, author VARCHAR, embeddings FLOAT[]);""")
    # We start the polling.
    now = time.time()
    cursor = poll_post_CSV()

    # We continue polling until we have scanned all the posts.
    while cursor != 0:
        cursor = poll_post_CSV(cursor)

    print("I'm done creating a CSV")

    # Now import the CSV file into the database.
    con.execute(
        "COPY story FROM 'data_export/story.csv' ( DELIMITER ',', HEADER TRUE );")
    print("I have successfully imported the CSV file into the database.")

    # We export the database to various formats.
    export_various_format()

    print("Polling total took {} ms".format((time.time() - now) * 1000))
