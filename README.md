# Apache Spark vs Ray — Distributed Computing Benchmark

Comparative study of **Apache Spark** and **Ray** for large-scale distributed data processing and machine learning workloads.

Developed as a research project for the **Analysis and Design of Information Systems** course at **NTUA ECE**.

## Overview

This project evaluates the performance and scalability of **Apache Spark** and **Ray** across multiple distributed workloads.

Both frameworks were deployed on a **5-node virtual machine cluster**, and experiments were executed using datasets of varying sizes, including workloads larger than the memory capacity of a single node.

The evaluation covers:

* Graph processing
* ETL pipelines
* K-Means clustering
* Linear Regression
* Distributed data processing
* Multi-node scalability

## Tech Stack

* **Python**
* **Apache Spark / PySpark**
* **Ray**
* **Hadoop HDFS**
* **YARN**
* **Spark MLlib**
* **Ray Data / Ray Train**
* **NumPy / Pandas**
* **Linux**

## Cluster Architecture

Experiments were executed on **5 virtual machines**, each configured with:

* **4 CPU cores**
* **8 GB RAM**

```text
                     Head Node
                  4 CPU · 8 GB
                       │
          ┌────────────┼────────────┐
          │            │            │
          ▼            ▼            ▼
      Worker 1     Worker 2     Worker 3
     4 CPU · 8GB  4 CPU · 8GB  4 CPU · 8GB
                                    │
                                    ▼
                                Worker 4
                               4 CPU · 8GB
```

Spark uses **HDFS + YARN** for distributed storage and resource management.

Ray runs as a distributed cluster consisting of a **head node and worker nodes**.

---

## Requirements

The project is intended to run on a Linux-based multi-node environment.

### Software

Install the following on the machines participating in the cluster:

* Python 3
* Java / OpenJDK
* Apache Hadoop
* Apache Spark
* Ray
* Git
* Python `venv` / `pip`

The Spark environment requires correctly configured:

* `JAVA_HOME`
* `HADOOP_HOME`
* `SPARK_HOME`
* HDFS
* YARN

All nodes should be able to communicate with each other over the network.

### Hardware

The original experiments used:

```text
Nodes:          5 Virtual Machines
CPU per node:   4 cores
RAM per node:   8 GB
```

An identical environment is not required, but benchmark results will vary depending on:

* CPU resources
* available memory
* number of workers
* network performance
* storage performance
* framework configuration

---

## Installation

Clone the repository:

```bash
git clone https://github.com/Kassaris/NTUA-BigData-Spark-Ray.git
cd NTUA-BigData-Spark-Ray
```

Create a Python virtual environment:

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Upgrade `pip`:

```bash
pip install --upgrade pip
```

Install the Python dependencies:

```bash
pip install -r requirements.txt
```

Install Ray with the required distributed data and training components:

```bash
pip install "ray[core,data,train,tune]"
```

Verify the installation:

```bash
python --version
ray --version
spark-submit --version
hadoop version
```

---

## Cluster Setup

### 1. Node Configuration

Configure one machine as the **head/master node** and the remaining machines as workers.

Each node should:

* have the required software installed
* be reachable from the other nodes
* have the appropriate hostnames/IP addresses configured
* use compatible versions of Python and the distributed frameworks

For Spark/Hadoop deployments, passwordless SSH between the relevant machines may also be required depending on the cluster configuration.

---

### 2. Hadoop / HDFS

Configure Hadoop so that HDFS is accessible from all cluster nodes.

Before running Spark experiments, start HDFS:

```bash
start-dfs.sh
```

Verify the running Hadoop processes:

```bash
jps
```

Depending on the node, processes such as the following should appear:

```text
NameNode
DataNode
SecondaryNameNode
```

Input datasets can then be uploaded to HDFS:

```bash
hdfs dfs -mkdir -p /data
hdfs dfs -put <local-file> /data/
```

Verify the uploaded files:

```bash
hdfs dfs -ls /data
```

---

### 3. YARN

Start the YARN resource manager:

```bash
start-yarn.sh
```

Verify the services:

```bash
jps
```

Expected processes may include:

```text
ResourceManager
NodeManager
```

---

### 4. Spark

Ensure that `SPARK_HOME` is configured correctly.

Start the Spark history server if execution history and metrics are required:

```bash
$SPARK_HOME/sbin/start-history-server.sh
```

A typical Spark experiment can then be submitted using:

```bash
spark-submit \
  --packages "ch.cern.sparkmeasure:spark-measure_2.12:0.23" \
  <script_folder>/<script>.py \
  <num_executors> \
  <hdfs:filepath>
```

For example:

```bash
spark-submit \
  --packages "ch.cern.sparkmeasure:spark-measure_2.12:0.23" \
  <experiment>.py \
  4 \
  hdfs:///data/<dataset>
```

The exact script and arguments depend on the experiment being executed.

---

### 5. Ray

Start Ray on the head node:

```bash
ray start \
  --head \
  --node-ip-address=<HEAD_NODE_IP> \
  --port=6379 \
  --dashboard-host=0.0.0.0 \
  --object-store-memory=2147483648 \
  --system-config='{"automatic_object_spilling_enabled": true, "object_spilling_threshold": 0.8}'
```

Then connect every worker node:

```bash
ray start --address=<HEAD_NODE_IP>:6379
```

Verify the cluster:

```bash
ray status
```

The output should show the resources available across the connected nodes.

Object spilling is enabled to allow Ray to handle workloads whose intermediate objects exceed the configured object-store memory.

To stop Ray:

```bash
ray stop
```

---

## Experiments

The project compares Spark and Ray across four main workload categories.

### 1. PageRank

PageRank is used to evaluate distributed iterative graph processing.

The experiment:

1. Loads a graph dataset
2. Distributes graph data across workers
3. Executes PageRank iterations
4. Calculates node importance scores
5. Extracts the highest-ranked nodes
6. Measures execution performance

Experiments are repeated using different graph sizes and cluster configurations.

The output includes the **10 nodes with the highest PageRank scores**.

---

### 2. Triangle Counting

Triangle Counting evaluates another common graph-processing workload.

A triangle exists when three vertices are mutually connected:

```text
A ───── B
 \     /
  \   /
    C
```

The experiment measures the frameworks' ability to process graph relationships and identify triangles across increasingly large graph datasets.

This workload is useful for comparing distributed graph-processing behavior because it requires significantly different data access patterns from PageRank.

---

### 3. ETL

The ETL benchmark evaluates large-scale tabular data processing.

The general pipeline is:

```text
CSV Dataset
     │
     ▼
   Extract
     │
     ▼
 Transform / Filter
     │
     ▼
 Sort / Process
     │
     ▼
    Output
```

The experiment focuses on operations representative of real-world data engineering workloads.

Performance is evaluated as dataset size and available cluster resources change.

---

### 4. K-Means Clustering

K-Means evaluates distributed unsupervised machine learning.

The workflow consists of:

```text
Dataset
   │
   ▼
Load & Preprocess
   │
   ▼
Initialize Clusters
   │
   ▼
Distributed K-Means
   │
   ▼
Cluster Assignments
```

Spark and Ray implementations are executed against datasets of different sizes to evaluate how distributed ML workloads scale across the cluster.

---

### 5. Linear Regression

Linear Regression is used as a supervised machine-learning workload.

The experiment includes:

* loading the dataset
* preprocessing features
* distributed model training
* prediction
* performance measurement

The comparison focuses primarily on execution behavior and scalability rather than treating the benchmark as a comparison of model quality.

---

## Benchmark Methodology

The experiments vary two important dimensions:

### Dataset Size

Multiple input sizes are used to examine how execution time changes as the workload grows.

Some datasets are intentionally large enough that processing them efficiently requires distributed resources.

### Number of Workers

Experiments are executed with different numbers of worker nodes.

Conceptually:

```text
Dataset Size ↑
      +
Worker Count ↑
      │
      ▼
Execution Time
Scalability
Resource Usage
```

This allows the project to investigate **horizontal scalability** rather than comparing Spark and Ray using only a single fixed cluster configuration.

---

## Running Experiments

### Spark

Make sure HDFS and YARN are running:

```bash
start-dfs.sh
start-yarn.sh
```

Then execute the required experiment:

```bash
spark-submit \
  --packages "ch.cern.sparkmeasure:spark-measure_2.12:0.23" \
  <script_folder>/<script>.py \
  <num_executors> \
  <hdfs:filepath>
```

Example workflow:

```text
Start HDFS
    ↓
Start YARN
    ↓
Upload Dataset
    ↓
Select Experiment
    ↓
Select Executor Count
    ↓
spark-submit
    ↓
Collect Metrics
```

### Ray

Start the head node:

```bash
ray start --head --node-ip-address=<HEAD_NODE_IP> --port=6379
```

Connect the required workers:

```bash
ray start --address=<HEAD_NODE_IP>:6379
```

Verify available resources:

```bash
ray status
```

Run an experiment:

```bash
python3 <script_folder>/<script>.py <hdfs:filepath>
```

Example workflow:

```text
Start Ray Head
    ↓
Connect Workers
    ↓
Verify Cluster
    ↓
Select Dataset
    ↓
Run Experiment
    ↓
Collect Metrics
```

---

## Reproducing Benchmarks

For meaningful comparisons between Spark and Ray, experiments should be executed under equivalent conditions.

Keep the following constant whenever possible:

* cluster hardware
* dataset
* number of workers
* dataset location
* preprocessing
* algorithm parameters
* number of iterations

Before recording results, it is also recommended to verify cluster state:

```bash
# Hadoop / Spark
jps

# HDFS
hdfs dfsadmin -report

# Ray
ray status
```

Benchmark results should be interpreted within the context of the specific hardware and software configuration.

---

## Project Structure

```text
NTUA-BigData-Spark-Ray/
│
├── data/           # Dataset generation and experiment data
├── documents/      # Documentation and experimental results
├── scripts/        # Spark and Ray implementations
│
├── requirements.txt
├── LICENSE
└── README.md
```

---

## Engineering Focus

The project provided hands-on experience with:

* Distributed systems
* Apache Spark / PySpark
* Ray
* Hadoop HDFS
* YARN
* Multi-node cluster configuration
* Graph algorithms
* ETL pipelines
* Distributed machine learning
* Performance benchmarking
* Horizontal scalability
* Resource management
* Memory management and object spilling
* Large-scale data processing

## Academic Context

Developed by a **2-person team** for the **Analysis and Design of Information Systems** course at the **National Technical University of Athens, School of Electrical and Computer Engineering**.

The objective was to experimentally compare **Apache Spark and Ray** across different distributed workloads and investigate their performance and scalability characteristics.

## Authors

* **Nikolaos Kassaris**
* **Konstantinos Vougias**

## Disclaimer

This repository is an academic research project.

Benchmark results depend heavily on the underlying hardware, cluster configuration, framework versions, dataset characteristics and workload parameters. Results should therefore not be interpreted as universal performance claims about Apache Spark or Ray.

---

**Python · Apache Spark · Ray · Hadoop · HDFS · YARN · Distributed ML**

*Distributed Computing & Big Data Research Project — NTUA ECE*
