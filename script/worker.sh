# This script is used to launch the worker available at main.py and should be run from the root of the project
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
poetry run rq worker --url "redis://:$REDIS_PASSWORD@127.0.0.1:6379"
