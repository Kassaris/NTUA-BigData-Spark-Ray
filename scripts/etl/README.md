This folder contains various ETL scripts. All nodes must access our hdfs in *in hdfs://okeanos-master:54310*

Spark scripts take as command line argument the number of executors and the hdfs path of the file:

*spark-submit --packages "ch.cern.sparkmeasure:spark-measure_2.12:0.23" <script_folder/script> <num_executors> <hdfs:filepath>*

Ray scripts take as command line argument the hdfs path of the file:

*python3 <script_folder>/<script>.py <hdfs:filepath>*

In sort_ray.py, we enabled push-based shuffle.
