## CSV Data Generation

This folder contains the python and bash scripts necessary to generate a large size CSV dataset with 
classificiation relevant schema and upload it to our HDFS cluster. To run the necessary commands run:


```bash
chmod +x generate_data_to_hdfs.sh
./generate_to_hdfs.sh <num_samples> </path/in/hdfs.csv>
```

This bash script utilizes the *data_generator.py* script to write to stdout and then redirect the output to an hdfs file.

---

## Graph Data Retrieval & Data Generation

We found real-world graphs from the **KONECT Project** ([KONECT Website](http://konect.cc/)) and the **Stanford SNAP Project** ([Stanford SNAP Website](https://snap.stanford.edu/)).

### Steps to Load Graph Data into HDFS:

1. Download the graph tar file using:
   ```bash
   wget <link>
   ```

2. Extract the data and remove the header:
   ```bash
   tar -xvf <tar>
   # Additional CLI operations to remove the header
   ```

3. Upload the processed file to HDFS:
   ```bash
   hdfs dfs -put <filepath> <hdfs:filepath>
   ```

### Generating Small-World and Scale-Free Graphs:

You can generate these graphs using the `generate_graph` scripts, which utilize the **NetworkX** library.

1. Run the script to generate a Small-World or Scale-Free graph:
   ```bash
   python generate_graph_<sf/sw>.py <num_samples>
   ```
   This will generate a graph with **(1000 × num_samples) nodes**.

2. Upload the generated graph to HDFS:
   ```bash
   hdfs dfs -put <filepath> <hdfs:filepath>
   ```
   ---

   ## Hand Gesture Detection System Dataset

We found real-world images on **Kaggle** ([Kaggle Website](https://www.kaggle.com/)).

### Steps to Load the Dataset into HDFS:

1. Download the dataset zip directly to HDFS using the following command:
   ```bash
   kaggle datasets download marusagar/hand-gesture-detection-system
   ```
   
2. Extract the data into the desired directory:
   ```bash
   unzip hand-gesture-detection-system.zip -d hand-gesture-detection-system
   ```





