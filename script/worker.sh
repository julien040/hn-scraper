#!/bin/bash

# This script is used to launch the worker available at main.py and should be run from the root of the project
poetry shell
doppler run --command "rq worker --url redis://:$(printenv REDIS_PASSWORD)@127.0.0.1:6379"
