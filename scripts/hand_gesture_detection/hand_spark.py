from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
import argparse
import sys
from PIL import Image
from pyspark.sql import Row

parser = argparse.ArgumentParser()
parser.add_argument('--csv', type=str, required=True, help='Path to train or val CSV on HDFS')
parser.add_argument('--root', type=str, required=True, help='Root folder for image data on HDFS')

args = parser.parse_args()

spark = SparkSession \
    .builder \
    .appName("aggregate_data") \
    .master("yarn") \
    .config("spark.executor.instances", args.num_executors) \
    .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23") \
    .getOrCreate()


def process_image(img_path):
    image = Image.open(img_path).convert('L')
    return np.array(image).flatten()


csv_data = spark.read.option("header", "true").csv(args.csv)
image_folders = csv_data.rdd.map(lambda row: row[0]).collect()


image_paths = []
hdfs_root_path = args.root

def collect_images(folder):
    folder_path = os.path.join(hdfs_root_path, folder)
    images = spark.sparkContext.binaryFiles(folder_path)
    return [img[0] for img in images.take(30)]

for folder in image_folders:
    image_paths.extend(collect_images(folder))

image_features_rdd = spark.sparkContext.parallelize(image_paths) \
    .map(lambda img_path: process_image(img_path))

image_features = image_features_rdd.collect()

for idx, feature in enumerate(image_features):
    gesture = image_paths[idx].split(os.path.sep)[-2]  # Get the folder name, which is the gesture label
    print(f"Recognized Gesture: {gesture} for Image: {image_paths[idx]}")

spark.stop()
