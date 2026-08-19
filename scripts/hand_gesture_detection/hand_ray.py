import ray
import os
import pandas as pd
import argparse
import numpy as np
import time
import sys
from hdfs import InsecureClient  
from PIL import Image  
import pyarrow.fs  
from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()
parser.add_argument('--csv', type=str, required=True, help='Path to train or val CSV on HDFS')
parser.add_argument('--root', type=str, required=True, help='Root folder for image data on HDFS')
args = parser.parse_args()

ray.init(address="auto")

@ray.remote
class ImageProcessor:
    def __init__(self, transform=None):
        self.transform = transform
    
    def process_image(self, img_path):
            image = Image.open(img_path).convert('L')
            if self.transform:
                image = self.transform(image)
            return np.array(image).flatten()  

hdfs_address = "hdfs://okeanos-master:54310"  
hdfs_fs = pyarrow.fs.HadoopFileSystem.from_uri(hdfs_address) 

ds = ray.data.read_csv(args.csv, filesystem=hdfs_fs) 
csv_data = ds.to_pandas() 

image_folders = csv_data.iloc[:, 0].tolist()
image_paths = []

for folder in image_folders:
    print(folder)
    folder_path = os.path.join(args.root, folder) 
    file_info = hdfs_fs.get_file_info(pyarrow.fs.FileSelector(folder_path))
    image_files = [f.path for f in file_info if f.is_file] 
    image_paths.extend(image_files)  

num_actors = 5
processors = [ImageProcessor.remote() for _ in range(num_actors)]


start_time = time.time()

tasks = []
for idx, img_path in enumerate(image_paths):
    actor = processors[idx % num_actors] 
    tasks.append(actor.process_image.remote(img_path))


features = ray.get(tasks)

end_time = time.time()
runtime = end_time - start_time

print(f"Runtime: {runtime:.2f} seconds")
for idx, img_path in enumerate(image_paths):
    gesture = os.path.basename(os.path.dirname(img_path)) 
    print(f"Recognized Gesture: {gesture} for Image: {img_path}")
