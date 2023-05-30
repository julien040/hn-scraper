#!/bin/bash

# This script is used to launch the scheduler available at main.py and should be run from the root of the project
poetry shell
doppler run --command "python main.py"
