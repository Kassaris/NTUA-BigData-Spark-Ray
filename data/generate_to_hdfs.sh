#!/bin/bash

NUM_SAMPLES=${1:-5000}
HDFS_DATA_PATH=${2:-/data/generated_data/generated_data.csv}
python3 data_generator.py "$NUM_SAMPLES" | hdfs dfs -put - "$HDFS_DATA_PATH"
