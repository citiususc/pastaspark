#!/bin/bash

SPARK_COMMAND="spark-submit --master yarn --deploy-mode cluster"
DRIVER_MEM="25G"
DRIVER_MEM_HUGE="25G"
DRIVER_MEM_BIG="15G"
EXEC_MEM="5G"

NUM_EXECUTORS="1"
DRIVER_CORES="4"
EXECUTOR_CORES="1"
ARCHIVES="pasta.zip"
PY_FILES="pasta.zip,/mnt/gluster/drv0/home/usc/ec/jam/.local/lib/python2.7/site-packages/DendroPy-3.12.3-py2.7.egg"

INPUT_DATA_BIG="/mnt/gluster/drv0/home/usc/ec/jam/Genomica/pasta/data/16S.B.ALL/R0/cleaned.alignment.fasta"
INPUT_TREE_BIG="/mnt/gluster/drv0/home/usc/ec/jam/Genomica/pasta/data/16S.B.ALL/R0/16S.B.ALL.reference.nwk"

INPUT_DATA_MEDIUM="/mnt/gluster/drv0/home/usc/ec/jam/Genomica/pasta/data/16S.T/R0/cleaned.alignment.fasta"
INPUT_TREE_MEDIUM="/mnt/gluster/drv0/home/usc/ec/jam/Genomica/pasta/data/16S.T/R0/16S.T.reference.nwk"

INPUT_DATA_HUGE="/mnt/gluster/drv0/home/usc/ec/jam/Genomica/pasta/data/SILVA_123.1_LSUParc_tax_silva_trunc.fasta"

INPUT_DATA_HUGE1="/mnt/gluster/drv0/home/usc/ec/jam/Genomica/pasta/data/RNASim/50000/1/model/true.fasta"
INPUT_TREE_HUGE1="/mnt/gluster/drv0/home/usc/ec/jam/Genomica/pasta/data/RNASim/50000/1/model/true.tt"

for i in `seq 1 3`;
do

	for NUM_EXECUTORS in 64 32 16 8;
	do

		#MEDIUM
		#echo "$SPARK_COMMAND --driver-memory $DRIVER_MEM --executor-memory $EXEC_MEM --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --executor-cores $EXECUTOR_CORES --archives $ARCHIVES --py-files $PY_FILES run_pasta.py --temporaries=./ -i $INPUT_DATA_MEDIUM -t $INPUT_TREE_MEDIUM --num-cpus=$DRIVER_CORES --num-cpus-spark=$EXECUTOR_CORES --num-partitions=$NUM_EXECUTORS"

		#$SPARK_COMMAND --driver-memory $DRIVER_MEM --executor-memory $EXEC_MEM --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --executor-cores $EXECUTOR_CORES --archives $ARCHIVES --py-files $PY_FILES run_pasta.py --temporaries=./ -i $INPUT_DATA_MEDIUM -t $INPUT_TREE_MEDIUM --num-cpus=$DRIVER_CORES --num-cpus-spark=$EXECUTOR_CORES --num-partitions=$NUM_EXECUTORS

		#BIG
		echo "$SPARK_COMMAND --name PASTA_16S_$NUM_EXECUTORS --driver-memory $DRIVER_MEM_BIG --executor-memory $EXEC_MEM --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --executor-cores $EXECUTOR_CORES --archives $ARCHIVES --py-files $PY_FILES run_pasta.py --temporaries=./ -i $INPUT_DATA_BIG -t $INPUT_TREE_BIG --num-cpus=$DRIVER_CORES --num-cpus-spark=$EXECUTOR_CORES --num-partitions=$NUM_EXECUTORS"

		$SPARK_COMMAND --name PASTA_16S_$NUM_EXECUTORS --driver-memory $DRIVER_MEM_BIG --executor-memory $EXEC_MEM --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --executor-cores $EXECUTOR_CORES --archives $ARCHIVES --py-files $PY_FILES run_pasta.py --temporaries=./ -i $INPUT_DATA_BIG -t $INPUT_TREE_BIG --num-cpus=$DRIVER_CORES --num-cpus-spark=$EXECUTOR_CORES --num-partitions=$NUM_EXECUTORS
		
		hdfs dfs -rm -r pastajob*

		echo "$SPARK_COMMAND --name PASTA_50k_$NUM_EXECUTORS --driver-memory $DRIVER_MEM_HUGE --executor-memory $EXEC_MEM --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --executor-cores $EXECUTOR_CORES --archives $ARCHIVES --py-files $PY_FILES --conf \"spark.memory.fraction=0.9\" --conf \"spark.memory.storageFraction=0.1\" --conf \"spark.akka.frameSize=512\"  run_pasta.py --temporaries=./ -i $INPUT_DATA_HUGE1 -t $INPUT_TREE_HUGE1 -d RNA --num-cpus=$DRIVER_CORES --num-cpus-spark=$EXECUTOR_CORES --num-partitions=$NUM_EXECUTORS"

                $SPARK_COMMAND --name PASTA_50k_$NUM_EXECUTORS --driver-memory $DRIVER_MEM_HUGE --executor-memory $EXEC_MEM --num-executors $NUM_EXECUTORS --driver-cores $DRIVER_CORES --executor-cores $EXECUTOR_CORES --archives $ARCHIVES --py-files $PY_FILES --conf "spark.memory.fraction=0.9"  --conf "spark.memory.storageFraction=0.1" --conf "spark.akka.frameSize=512"  run_pasta.py --temporaries=./ -i $INPUT_DATA_HUGE1 -t $INPUT_TREE_HUGE1 -d RNA --num-cpus=$DRIVER_CORES --num-cpus-spark=$EXECUTOR_CORES --num-partitions=$NUM_EXECUTORS

		hdfs dfs -rm -r pastajob*
	done
done

