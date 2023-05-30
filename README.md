# Hacker News scraper

Fetch last HN posts and save them in a database.

## Technical stack

### Scheduler

The project employs [RQ](http://python-rq.org/) to schedule the scraping of HN posts. 

Every five minutes, a watcher checks for new posts to fetch and adds jobs to the queue. Additionally, it fetches the first 100 posts every five minutes to ensure the database is up-to-date.

### Scraper

The scraper listens to the queue and fetches posts. It stores them in the database if they are stories, not jobs, polls, etc. It does not save posts without URLs, such as "Ask HN".

When fetched, the URL is scraped by Diffbot to get the article content. This content is then sent to the OpenAI API to get the article embedding. The embedding is then stored in the database, but only if the article isn't already in the database.

### Database

The database is a Redis database.
It contains 2 sets:
- db0 contains the queue
- db1 contains the posts

Posts are prefixed by hn:<id>







