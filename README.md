# NTUA-BigData-Spark-Ray
Comparison of Python's scaling frameworks, Ray and Apache Spark as an assignment for the *Analysis and Design of Information Systems* class

## TEAM 28

## Researchers
- Nikolaos Kassaris          03119188 [el19188](https://github.com/ntua-el19188/)
- Konstantinos Vougias       03119144 [NtinosVg](https://github.com/NtinosVg/)

## Project Overview
This repository contains the necessary data generation instructions and a set of scripts used for our research on a comparative analysis of the Ray and Apache Spark frameworks.

## Research Objective
The goal of this research is to provide valuable insights into distributed computing using Ray and Apache Spark, highlighting their strengths and weaknesses across various workloads.
Through a series of experiments, we evaluate the performance of these frameworks on different large-scale computational tasks.
Our experiments focus on the following distributed computing operations:
- **Graph Operations** (PageRank, Triangle Counting): 
    -   Implemented the PageRank algorithm on different graph sizes to measure execution time and extract the top 10 most significant nodes in             each graph.
    -   Performed triangle counting on multiple graphs.
- **ETL Operations** on large datasets (CSV processing)
    -   Measured the execution time, efficiency and performance of extract, transform, load, sort operations on large-scale CSV datasets.
- **K-means clustering**:
    -   Measure the frameworks on k-means clustering, using large csv datasets of varying size on performance and clustering quality.
- **Linear Regression prediction**:
    -   Implemented distributed linear regression to predict target variables based on input features.
    -   Evaluated the frameworks based on training time, prediction accuracy, and scalability across large datasets.

These tasks were executed with different number of nodes and utilized different dataset sizes and types in order to extract results regarding the frameworks's scalabilities.
For most of the operations we used datasets with sizes too large to fit in main memory of a single machine. Our cluster consisted of 5 virtual machines each equipped with 4 CPUs and 8GB RAM.

(More complex jobs, like hand gesture detection could also be used to assess the differences in the frameworks but due to time constraints such results were not published)

## Requirements
In each vm we setup a python virtual environment and installed the necessary python packages. A list of these packages is available on the requirements.txt file. 
After activating the venv virtual environment install them using the command:

`pip install -r requirements.txt`

To install some additional Ray packages run the command:

`pip install ray[core,data,train,tune]`

## Installation 
In order to setup the Hadoop Distributed File System, Yarn and Spark environment on all nodes we followed the guide issued for the Ntua Advanced DB class:

https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing

For Ray just install the necessary python packages.

## Setup
To start the HDFS and connect my nodes to a YARN cluster run
`start-dfs`, `start-yarn.sh` and `$SPARK_HOME/sbin/start-history-server.sh` on the head node.

To start a Ray head node and initialize the cluster (with enabled object spilling):
```bash
ray start --head --node-ip-address=[head-node-private-ip-address] --port=6379 --dashboard-host=0.0.0.0 --object-store-memory=2147483648 --system-config='{"automatic_object_spilling_enabled": true, "object_spilling_threshold": 0.8}'
```
To attach a worker node on the Ray Cluster after setting up the head node:
```bash
ray start --address=[head-node-private-ip-address]
```

## Experiments 


**For spark script ,start HDFS, YARN, and Spark cluster and run the spark script**:
  
```bash
spark-submit --packages "ch.cern.sparkmeasure:spark-measure_2.12:0.23" <script_folder>/<script> <num_executors> <hdfs:filepath> 
```

**For ray scripts, start the Ray cluster and run the ray script**:
      
```bash
python3 <script_folder/script>.py <hdfs:filepath>
```

