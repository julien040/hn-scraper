from persistence import rPost, redis_queue, redis_connection_queue
from rq.job import Job
from rq import Queue
from embeddings import add_embeddings_redis
import time


THRESHOLD = 100  # The minimum score of the post to compute embeddings.
SCAN_COUNT = 10000  # The number of posts to scan in Redis per iteration.
PATTERN = "hn:*"  # The pattern to use to scan Redis.


def poll_post_for_embedding(cursor: int = 0):
    """
    Poll the posts in Redis to compute their embeddings.
    If the post has already been added to the queue, 
    or if it has already been computed, we do nothing.

    To do so, we iterate over the posts in Redis using the SCAN command.
    """

    res_scan = rPost.scan(cursor=cursor, count=SCAN_COUNT)
    print("Polling posts for embeddings. Cursor: {}".format(cursor))
    print("{} posts to scan.".format(len(res_scan[1])))

    # We iterate over the posts to check if they have to be added to the queue.
    # or if they have already been computed.

    # Convert the bytes to strings.
    # We remove the "hn:" prefix.
    # We add the "embedding_" prefix.
    list_post_id = ["embedding_"+(id.decode("utf-8")[3:])
                    for id in res_scan[1]]

    # We iterate over the jobs to check if they have already been computed.
    # If not, we add them to "jobID_to_check_for_embedding".
    jobs = Job.fetch_many(list_post_id,
                          connection=redis_connection_queue)

    jobID_to_check_for_embedding = []
    for i in range(len(jobs)):
        if jobs[i] == None:
            # We add the job to the list of jobs to check for embeddings.
            # We remove the "embedding_" prefix.
            jobID_to_check_for_embedding.append(list_post_id[i][10:])

    # We iterate over the jobs to check if they have already been computed.
    # If not, we will push them to the queue.
    # To do so, we batch fetch if the post contains the "embeddings" field using a pipeline.

    jobID_to_check_for_score = []
    pipe = rPost.pipeline()
    for postID in jobID_to_check_for_embedding:
        pipe.hexists("hn:"+postID, "embeddings")

    # We execute the pipeline.
    res = pipe.execute()
    for i in range(len(res)):
        if res[i] == False:
            jobID_to_check_for_score.append(jobID_to_check_for_embedding[i])

    # We check if the score is above the threshold.
    # We batch fetch the score using a pipeline.
    jobID_to_push = []

    pipe = rPost.pipeline()
    for postID in jobID_to_check_for_score:
        pipe.hget("hn:"+postID, "score")

    # We execute the pipeline.
    res = pipe.execute()
    for i in range(len(res)):
        if res[i] != None and int(res[i]) >= THRESHOLD:
            jobID_to_push.append(jobID_to_check_for_score[i])

    # We batch push the job to the queue.
    # We use postID[3:] because we want to remove the "hn:" prefix.

    jobs_list = []
    for postID in jobID_to_push:
        jobs_list.append(

            Queue.prepare_data(add_embeddings_redis,
                               (postID,),
                               job_id="embedding_{}".format(postID),
                               result_ttl=10)
        )

    redis_queue.enqueue_many(jobs_list)

    print("I have enqueued {} jobs.\n".format(len(jobs_list)))

    # We call the function with the next cursor.
    if res_scan[0] != 0:
        poll_post_for_embedding(cursor=res_scan[0])


if __name__ == "__main__":
    # We start the polling.
    now = time.time()
    poll_post_for_embedding()
    print("Polling total took {} ms".format((time.time() - now) * 1000))
