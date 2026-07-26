#!/usr/bin/env bash

###############################################################################
# PageRank with Apache Pig
#
# Mirrors python/run.sh in structure and benchmark output format.
# Each call to Apache Pig executes ONE iteration (pig/pagerank.pig).
# Convergence checking reuses python/verify.py.
#
# Usage:
#   ./run.sh [dataset] [max_iterations]
#
# Examples:
#   ./run.sh                                      # graph.txt, 20 iterations
#   ./run.sh ../dataset/Wiki-Vote.txt 20
#
# Environment overrides (optional):
#   DAMPING=0.85   THRESHOLD=1e-6   HDFS_REPLICATION=1
#   HDFS_BLOCK_SIZE=134217728   HDFS_ROOT=/pagerank/pig
###############################################################################

set -euo pipefail

# -----------------------------------------------------------------------------
# Project paths
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PIG_SCRIPT="$SCRIPT_DIR/pagerank.pig"
VERIFY="$PROJECT_ROOT/python/verify.py"

OUTPUT_DIR="$PROJECT_ROOT/output/pig"
BENCHMARK_DIR="$PROJECT_ROOT/benchmark/pig"
LOG_DIR="$PROJECT_ROOT/log"

# -----------------------------------------------------------------------------
# Parameters  (same defaults as python/run.sh and the Setup Guide Section 6)
# -----------------------------------------------------------------------------

DATASET="${1:-$PROJECT_ROOT/dataset/graph.txt}"
MAX_ITERATIONS="${2:-20}"

DAMPING="${DAMPING:-0.85}"
THRESHOLD="${THRESHOLD:-1e-6}"

HDFS_REPLICATION="${HDFS_REPLICATION:-1}"
HDFS_BLOCK_SIZE="${HDFS_BLOCK_SIZE:-134217728}"   # 128 MB

HDFS_ROOT="${HDFS_ROOT:-/pagerank/pig}"
HDFS_INPUT="$HDFS_ROOT/input"
HDFS_OUTPUT="$HDFS_ROOT/output"

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

mkdir -p "$OUTPUT_DIR" "$BENCHMARK_DIR" "$LOG_DIR"

TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/pig_pagerank_$TIMESTAMP.log"

log() {
    echo "$(date '+%F %T') | $*" | tee -a "$LOG_FILE"
}

fail() {
    log "ERROR: $*"
    exit 1
}

# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------

command -v hadoop  >/dev/null 2>&1 || fail "hadoop command not found"
command -v hdfs    >/dev/null 2>&1 || fail "hdfs command not found"
command -v pig     >/dev/null 2>&1 || fail "pig command not found"
command -v python3 >/dev/null 2>&1 || fail "python3 command not found"

[[ -n "${HADOOP_HOME:-}" ]] || fail "HADOOP_HOME is not configured"

[[ -f "$DATASET"    ]] || fail "Dataset not found: $DATASET"
[[ -f "$PIG_SCRIPT" ]] || fail "pagerank.pig not found: $PIG_SCRIPT"
[[ -f "$VERIFY"     ]] || fail "verify.py not found: $VERIFY"

[[ "$MAX_ITERATIONS" =~ ^[0-9]+$ ]] \
    || fail "MAX_ITERATIONS must be an integer"
(( MAX_ITERATIONS > 0 )) \
    || fail "MAX_ITERATIONS must be greater than zero"

# Count valid (non-empty) records in the dataset.
NUM_NODES="$(
    python3 - "$DATASET" <<'PY'
import sys

count = 0

with open(sys.argv[1], "r", encoding="utf-8") as f:
    for line in f:
        if line.rstrip("\r\n"):
            count += 1

print(count)
PY
)"

(( NUM_NODES > 0 )) || fail "Dataset contains no nodes"

# Initial dangling mass: sum of (1/N) for every dangling node.
# A dangling node has an empty adjacency field.
INITIAL_DANGLING="$(
    python3 - "$DATASET" "$NUM_NODES" <<'PY'
import sys

filename   = sys.argv[1]
num_nodes  = int(sys.argv[2])
dangling   = 0

with open(filename, "r", encoding="utf-8") as f:
    for line in f:
        fields = line.rstrip("\r\n").split("\t")
        if not fields or not fields[0].strip():
            continue
        adj = fields[1].strip() if len(fields) >= 2 else ""
        if adj == "":
            dangling += 1

print(f"{dangling / num_nodes:.15f}")
PY
)"

log "PageRank Apache Pig"
log "Dataset          : $DATASET"
log "Nodes            : $NUM_NODES"
log "Initial dangling : $INITIAL_DANGLING"
log "Damping          : $DAMPING"
log "Threshold        : $THRESHOLD"
log "Max iterations   : $MAX_ITERATIONS"
log "Pig script       : $PIG_SCRIPT"

# -----------------------------------------------------------------------------
# Clean local results  (same files as python/run.sh)
# -----------------------------------------------------------------------------

rm -f "$OUTPUT_DIR"/iter*.txt
rm -f "$OUTPUT_DIR"/final.txt

rm -f "$BENCHMARK_DIR/time.csv"
rm -f "$BENCHMARK_DIR/convergence.csv"
rm -f "$BENCHMARK_DIR/dangling.txt"
rm -f "$BENCHMARK_DIR/convergence.txt"
rm -f "$BENCHMARK_DIR/converged.txt"
rm -f "$BENCHMARK_DIR/top20.csv"
rm -f "$BENCHMARK_DIR/summary.txt"

echo "Iteration,Seconds" > "$BENCHMARK_DIR/time.csv"

# -----------------------------------------------------------------------------
# Prepare HDFS
# -----------------------------------------------------------------------------

log "Checking HDFS"
hdfs dfs -ls / >/dev/null 2>&1 || fail "HDFS is unavailable"

log "Cleaning old HDFS output under $HDFS_ROOT"
hdfs dfs -rm -r -f "$HDFS_ROOT" >>"$LOG_FILE" 2>&1 || true
hdfs dfs -mkdir -p "$HDFS_INPUT"

# Preprocess: convert 2-column (Node<TAB>AdjacencyList) to
# 3-column (Node<TAB>InitialRank<TAB>AdjacencyList) with rank = 1/N.
#
# This lets pagerank.pig always receive a uniform 3-column input
# (no runtime format detection needed inside Pig).
#
# The preprocessed file is written to a temp path and removed after upload.

PREPROCESSED="/tmp/pig_pagerank_init_${TIMESTAMP}.txt"

log "Preprocessing dataset → 3-column format (initial rank = 1/$NUM_NODES)"

python3 - "$DATASET" "$NUM_NODES" "$PREPROCESSED" <<'PY'
import sys

filename, num_nodes, out_file = sys.argv[1], int(sys.argv[2]), sys.argv[3]
init_rank = 1.0 / num_nodes

with open(filename, "r", encoding="utf-8") as fin, \
     open(out_file,  "w", encoding="utf-8") as fout:
    for line in fin:
        line = line.rstrip("\r\n")
        if not line:
            continue
        parts = line.split("\t", 1)
        node  = parts[0].strip()
        adj   = parts[1].strip() if len(parts) >= 2 else ""
        # Always write a trailing TAB so dangling nodes (adj="") still
        # produce a proper 3-field record when loaded by PigStorage.
        fout.write(f"{node}\t{init_rank:.15f}\t{adj}\n")
PY

log "Uploading preprocessed dataset to HDFS: $HDFS_INPUT/graph.txt"

hdfs dfs \
    -Ddfs.blocksize="$HDFS_BLOCK_SIZE" \
    -put -f "$PREPROCESSED" "$HDFS_INPUT/graph.txt" \
    >>"$LOG_FILE" 2>&1

hdfs dfs -setrep -w "$HDFS_REPLICATION" \
    "$HDFS_INPUT/graph.txt" \
    >>"$LOG_FILE" 2>&1

rm -f "$PREPROCESSED"

# -----------------------------------------------------------------------------
# Iteration loop
# -----------------------------------------------------------------------------

CURRENT_INPUT="$HDFS_INPUT/graph.txt"
DANGLING="$INITIAL_DANGLING"

TOTAL_START="$(date +%s)"
FINAL_ITERATION=0
CONVERGED=0
LOCAL_CURRENT=""

for (( iteration = 1; iteration <= MAX_ITERATIONS; iteration++ )); do
    log "Starting iteration $iteration"

    CURRENT_OUTPUT="$HDFS_OUTPUT/iter$iteration"
    LOCAL_CURRENT="$OUTPUT_DIR/iter$iteration.txt"

    hdfs dfs -rm -r -f "$CURRENT_OUTPUT" >>"$LOG_FILE" 2>&1 || true

    ITERATION_START="$(date +%s)"

    # Run ONE PageRank iteration via Apache Pig.
    # -x mapreduce  : execute on the Hadoop cluster (not local mode).
    # -p KEY=VALUE  : substitute parameters inside pagerank.pig.
    pig -x mapreduce \
        -p INPUT="$CURRENT_INPUT" \
        -p OUTPUT="$CURRENT_OUTPUT" \
        -p NUM_NODES="$NUM_NODES" \
        -p DAMPING="$DAMPING" \
        -p DANGLING="$DANGLING" \
        "$PIG_SCRIPT" \
        2>&1 | tee -a "$LOG_FILE"

    ITERATION_END="$(date +%s)"
    ITERATION_SECONDS=$(( ITERATION_END - ITERATION_START ))

    echo "$iteration,$ITERATION_SECONDS" >> "$BENCHMARK_DIR/time.csv"

    # Combine all reducer part files into one local result file.
    hdfs dfs -cat "$CURRENT_OUTPUT"/part-* \
        > "$LOCAL_CURRENT"

    # Validate output and check convergence using python/verify.py.
    # verify.py writes:
    #   benchmark/pig/dangling.txt   - dangling mass for the next iteration
    #   benchmark/pig/converged.txt  - "1" if converged, "0" otherwise
    #   benchmark/pig/convergence.csv
    #   benchmark/pig/top20.csv
    if (( iteration == 1 )); then
        python3 "$VERIFY" \
            --current   "$LOCAL_CURRENT" \
            --iteration "$iteration" \
            --threshold "$THRESHOLD" \
            --benchmark-dir "$BENCHMARK_DIR" \
            2>&1 | tee -a "$LOG_FILE"
    else
        LOCAL_PREVIOUS="$OUTPUT_DIR/iter$(( iteration - 1 )).txt"

        python3 "$VERIFY" \
            --current   "$LOCAL_CURRENT" \
            --previous  "$LOCAL_PREVIOUS" \
            --iteration "$iteration" \
            --threshold "$THRESHOLD" \
            --benchmark-dir "$BENCHMARK_DIR" \
            2>&1 | tee -a "$LOG_FILE"
    fi

    DANGLING="$(tr -d '[:space:]' < "$BENCHMARK_DIR/dangling.txt")"
    CONVERGED="$(tr -d '[:space:]' < "$BENCHMARK_DIR/converged.txt")"

    FINAL_ITERATION="$iteration"

    log "Iteration time  : $ITERATION_SECONDS seconds"
    log "Next dangling   : $DANGLING"

    if [[ "$CONVERGED" == "1" ]]; then
        log "Convergence reached at iteration $iteration"
        break
    fi

    CURRENT_INPUT="$CURRENT_OUTPUT"
done

# -----------------------------------------------------------------------------
# Final files and summary
# -----------------------------------------------------------------------------

TOTAL_END="$(date +%s)"
TOTAL_SECONDS=$(( TOTAL_END - TOTAL_START ))

[[ -n "$LOCAL_CURRENT" && -f "$LOCAL_CURRENT" ]] \
    || fail "No final output was generated"

cp "$LOCAL_CURRENT" "$OUTPUT_DIR/final.txt"

FINAL_TOTAL_RANK="$(
    python3 - "$OUTPUT_DIR/final.txt" <<'PY'
import sys
total = 0.0
with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) >= 2:
            try:
                total += float(parts[1])
            except ValueError:
                pass
print(f"{total:.15f}")
PY
)"

{
    echo "Implementation=Apache Pig"
    echo "Dataset=$DATASET"
    echo "Nodes=$NUM_NODES"
    echo "Damping=$DAMPING"
    echo "Threshold=$THRESHOLD"
    echo "Iterations=$FINAL_ITERATION"
    echo "Converged=$CONVERGED"
    echo "TotalPageRank=$FINAL_TOTAL_RANK"
    echo "TotalTimeSeconds=$TOTAL_SECONDS"
    echo "FinalOutput=$OUTPUT_DIR/final.txt"
    echo "LogFile=$LOG_FILE"
} > "$BENCHMARK_DIR/summary.txt"

log "PageRank completed"
log "Iterations      : $FINAL_ITERATION"
log "Converged       : $CONVERGED"
log "Total PageRank  : $FINAL_TOTAL_RANK"
log "Total time      : $TOTAL_SECONDS seconds"
log "Final output    : $OUTPUT_DIR/final.txt"
log "Benchmark       : $BENCHMARK_DIR"
log "Log             : $LOG_FILE"
