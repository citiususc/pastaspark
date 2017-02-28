What is PASTASpark about?
===
PASTASpark is a parallel distributed memory implementation of PASTA by using Apache Spark as engine.

INSTALLATION
===
**Dependencies**: 

1. You need to have python 2.7 or later.
2. You need to have java installed (required for Opal, which is by the default used in PASTA for merging small alignments).
3. You need to have a cluster with Hadoop/YARN and Spark installed and running. We have tested PASTASpark with Hadoop 2.7.1 and Spark 1.6.1.
4. PASTASpark requires a shared folder among the computing nodes to store the results in the cluster.

PASTASpark only works in Linux machines. To install it, you have to follow these steps:
1. Clone the repository:
```
git clone https://github.com/citiususc/pastaspark.git
```
2. Enter the created directory and run the install command:

```
cd pastaspark
python setup.py develop --user
```

Working modes
===
PASTASpark can be executed as the original PASTA or on a YARN/Spark cluster. In this way, if you launch PASTASpark within a Spark context, it will be executed on your Spark cluster. You can find more information about this topic in the next section.

Execution examples
===
A basic example of how to execute PASTASpark in your local machine with a working Spark setup is:
```
spark-submit --master local run_pasta.py -i data/small.fasta -t data/small.tree
```

The following is an example of how to launch PASTASpark using a bash script and taking as input the files stored in the `data` directory:
```
#!/bin/bash

SPARK_COMMAND="spark-submit --master yarn --deploy-mode cluster"
DRIVER_MEM="25G"
EXEC_MEM="5G"

CURRENT_DIR=`pwd`
HOME="/home/jmabuin"

NUM_EXECUTORS="8"
DRIVER_CORES="4"
EXECUTOR_CORES="1"
ARCHIVES="pasta.zip"
PY_FILES="pasta.zip,$HOME/.local/lib/python2.7/site-packages/DendroPy-3.12.3-py2.7.egg"

INPUT_DATA="$CURRENT_DIR/data/small.fasta"
INPUT_TREE="$CURRENT_DIR/data/small.tree"

$SPARK_COMMAND --name PastaSpark_Small_8Exec --driver-memory $DRIVER_MEM --executor-memory $EXEC_MEM --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --executor-cores $EXECUTOR_CORES --archives $ARCHIVES --py-files $PY_FILES run_pasta.py --temporaries=./ -i $INPUT_DATA -t $INPUT_TREE --num-cpus=$DRIVER_CORES --num-cpus-spark=$EXECUTOR_CORES --num-partitions=$NUM_EXECUTORS

```
To see the original PASTA documentation, click [here](README_PASTA.md).