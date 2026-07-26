# PageRank on Hadoop - Apache Pig

PageRank implementation using Apache Pig on Hadoop MapReduce.  
Each call to `pagerank.pig` executes **one iteration**; `run.sh` drives the full loop.

## Requirements

| Tool | Minimum version |
|---|---|
| Apache Hadoop | 3.x (HDFS + YARN running) |
| Apache Pig | 0.17+ |
| Python 3 | 3.6+ |
| Java | 8+ (required by Pig/Hadoop) |

Environment variables must be set:

```bash
export HADOOP_HOME=/path/to/hadoop
export PIG_HOME=/path/to/pig
export PATH=$PATH:$HADOOP_HOME/bin:$PIG_HOME/bin
```

Verify the cluster is healthy before running:

```bash
jps                   # NameNode, DataNode, ResourceManager, NodeManager
hdfs dfs -ls /
yarn node -list
```

## Files

| File | Role |
|---|---|
| `pagerank.pig` | One PageRank iteration (Pig Latin) |
| `run.sh` | Orchestration: preprocess → HDFS upload → iteration loop → benchmark |

## Run

```bash
cd pagerank/pig
chmod +x run.sh

# Small graph (4 nodes) — quick sanity check
./run.sh

# WikiVote dataset
./run.sh ../dataset/Wiki-Vote.txt 20
```

### Parameters

```
./run.sh [dataset] [max_iterations]
```

| Argument | Default | Description |
|---|---|---|
| `dataset` | `dataset/graph.txt` | Path to local graph file (`Node<TAB>AdjacencyList`) |
| `max_iterations` | `20` | Maximum number of iterations |

Additional parameters can be set via environment variables:

```bash
DAMPING=0.85 THRESHOLD=1e-6 ./run.sh ../dataset/Wiki-Vote.txt
```

| Variable | Default | Description |
|---|---|---|
| `DAMPING` | `0.85` | Damping factor |
| `THRESHOLD` | `1e-6` | Convergence threshold (max-delta) |
| `HDFS_REPLICATION` | `1` | HDFS replication factor |
| `HDFS_BLOCK_SIZE` | `134217728` | HDFS block size (128 MB) |
| `HDFS_ROOT` | `/pagerank/pig` | HDFS root path |

## What `run.sh` does

1. Preprocesses the 2-column graph (`Node<TAB>Adj`) into 3-column format (`Node<TAB>1/N<TAB>Adj`) and uploads to HDFS.
2. Calls `pig -x mapreduce -p INPUT=… -p OUTPUT=… -p NUM_NODES=… -p DAMPING=… -p DANGLING=…` for each iteration.
3. Downloads each iteration's output from HDFS.
4. Runs `python/verify.py` to compute dangling mass and check convergence.
5. Stops early if `MaxDelta < THRESHOLD`; otherwise runs up to `max_iterations`.

## Output

| Path | Contents |
|---|---|
| `output/pig/iter1.txt` … | Per-iteration results (`Node<TAB>Rank<TAB>Adj`) |
| `output/pig/final.txt` | Copy of the last iteration's result |
| `benchmark/pig/time.csv` | Per-iteration execution time |
| `benchmark/pig/convergence.csv` | TotalPageRank, MaxDelta, Converged per iteration |
| `benchmark/pig/top20.csv` | Top 20 nodes by final PageRank |
| `benchmark/pig/summary.txt` | Run summary (nodes, iterations, total time, etc.) |
| `log/pig_pagerank_<timestamp>.log` | Full Pig + HDFS log |
