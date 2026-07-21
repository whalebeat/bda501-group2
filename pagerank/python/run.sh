#!/bin/bash

###############################################################################
# Hadoop Streaming PageRank
###############################################################################

set -e

# ---------------------------------------------------------------------------

HADOOP_HOME=${HADOOP_HOME:-$HOME/hadoop}

STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" \
    -name "hadoop-streaming-*.jar" | head -n 1)

INPUT_FILE=pagerank.txt

HDFS_DIR=/pagerank

ITERATIONS=10

DAMPING=0.85

# ---------------------------------------------------------------------------

if [ ! -f "$INPUT_FILE" ]; then
    echo "Input file not found."
    exit 1
fi

if [ ! -f mapper.py ]; then
    echo "mapper.py not found."
    exit 1
fi

if [ ! -f reducer.py ]; then
    echo "reducer.py not found."
    exit 1
fi

chmod +x mapper.py
chmod +x reducer.py

NUM_NODES=$(wc -l < "$INPUT_FILE")

echo "Nodes       : $NUM_NODES"
echo "Iterations  : $ITERATIONS"
echo "Damping     : $DAMPING"

export NUM_NODES
export DAMPING

# ---------------------------------------------------------------------------

echo
echo "Cleaning HDFS..."

hdfs dfs -mkdir -p $HDFS_DIR

hdfs dfs -rm -r -f ${HDFS_DIR}/output >/dev/null 2>&1 || true

hdfs dfs -rm -f ${HDFS_DIR}/pagerank.txt >/dev/null 2>&1 || true

hdfs dfs -put "$INPUT_FILE" ${HDFS_DIR}/

CURRENT_INPUT=${HDFS_DIR}/pagerank.txt

# ---------------------------------------------------------------------------

for ((i=1;i<=ITERATIONS;i++))
do

    OUTPUT=${HDFS_DIR}/output/iter$(printf "%02d" $i)

    echo
    echo "==============================="
    echo "Iteration $i"
    echo "==============================="

    hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1 || true

    hadoop jar "$STREAMING_JAR" \
        -D mapreduce.job.name="PageRank Iteration $i" \
        -cmdenv NUM_NODES=$NUM_NODES \
        -cmdenv DAMPING=$DAMPING \
        -files mapper.py,reducer.py \
        -mapper mapper.py \
        -reducer reducer.py \
        -input "$CURRENT_INPUT" \
        -output "$OUTPUT"

    CURRENT_INPUT=$OUTPUT/part-00000

done

echo
echo "Finished."

echo
echo "Final Result"

hdfs dfs -cat "$CURRENT_INPUT"