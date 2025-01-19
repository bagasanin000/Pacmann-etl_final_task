#!/bin/bash

# Virtual Environment Path
VENV_PATH="/root/etl_final_task/etl-project/bin/activate"

# Activate venv
source "$VENV_PATH"

# set python script
PYTHON_SCRIPT="/root/etl_final_task/etl_luigi.py"

# run python script
python "$PYTHON_SCRIPT" >> /root/etl_final_task/log/logfile.log 2>&1
