#!/usr/bin/env bash

###############################################################################
# PageRank with Hadoop Streaming
###############################################################################

set -euo pipefail

# -----------------------------------------------------------------------------
# Project paths
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MAPPER="$SCRIPT_DIR/mapper.py"
REDUCER="$SCRIPT_DIR/reducer.py"
VERIFY="$SCRIPT_DIR/verify.py"

OUTPUT_DIR="$PROJECT_ROOT/output"
BENCHMARK_DIR="$PROJECT_ROOT/benchmark"
LOG_DIR="$PROJECT_ROOT/log"

# -----------------------------------------------------------------------------
# Parameters
# -----------------------------------------------------------------------------

DATASET="${1:-$PROJECT_ROOT/dataset/graph.txt}"
MAX_ITERATIONS="${2:-20}"

DAMPING="${DAMPING:-0.85}"
THRESHOLD="${THRESHOLD:-1e-6}"
NUM_REDUCERS="${NUM_REDUCERS:-1}"

HDFS_REPLICATION="${HDFS_REPLICATION:-1}"
HDFS_BLOCK_SIZE="${HDFS_BLOCK_SIZE:-134217728}"

HDFS_ROOT="${HDFS_ROOT:-/pagerank}"
HDFS_INPUT="$HDFS_ROOT/input"
HDFS_OUTPUT="$HDFS_ROOT/output"

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

mkdir -p "$OUTPUT_DIR" "$BENCHMARK_DIR" "$LOG_DIR"

TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/python_pagerank_$TIMESTAMP.log"

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

command -v hadoop >/dev/null 2>&1 \
    || fail "hadoop command not found"

command -v hdfs >/dev/null 2>&1 \
    || fail "hdfs command not found"

command -v python3 >/dev/null 2>&1 \
    || fail "python3 command not found"

[[ -n "${HADOOP_HOME:-}" ]] \
    || fail "HADOOP_HOME is not configured"

[[ -f "$DATASET" ]] \
    || fail "Dataset not found: $DATASET"

[[ -f "$MAPPER" ]] \
    || fail "mapper.py not found"

[[ -f "$REDUCER" ]] \
    || fail "reducer.py not found"

[[ -f "$VERIFY" ]] \
    || fail "verify.py not found"

[[ "$MAX_ITERATIONS" =~ ^[0-9]+$ ]] \
    || fail "MAX_ITERATIONS must be an integer"

(( MAX_ITERATIONS > 0 )) \
    || fail "MAX_ITERATIONS must be greater than zero"

STREAMING_JAR="$(
    find "$HADOOP_HOME" \
        -type f \
        -name 'hadoop-streaming-*.jar' \
        2>/dev/null |
    head -n 1
)"

[[ -f "$STREAMING_JAR" ]] \
    || fail "Hadoop Streaming JAR not found"

chmod +x "$MAPPER" "$REDUCER" "$VERIFY"

# Count valid, non-empty graph records.
NUM_NODES="$(
    python3 - "$DATASET" <<'PY'
import sys

count = 0

with open(sys.argv[1], "r", encoding="utf-8") as file:
    for raw_line in file:
        if raw_line.rstrip("\r\n"):
            count += 1

print(count)
PY
)"

(( NUM_NODES > 0 )) \
    || fail "Dataset contains no nodes"

# Initial dangling mass:
# each dangling node initially has rank 1/N.
INITIAL_DANGLING="$(
    python3 - "$DATASET" "$NUM_NODES" <<'PY'
import sys

filename = sys.argv[1]
num_nodes = int(sys.argv[2])
dangling_nodes = 0

with open(filename, "r", encoding="utf-8") as file:
    for raw_line in file:
        fields = raw_line.rstrip("\r\n").split("\t")

        if not fields or not fields[0].strip():
            continue

        adjacency = fields[1].strip() if len(fields) >= 2 else ""

        if adjacency == "":
            dangling_nodes += 1

print(f"{dangling_nodes / num_nodes:.15f}")
PY
)"

log "PageRank Hadoop Streaming"
log "Dataset          : $DATASET"
log "Nodes            : $NUM_NODES"
log "Initial dangling : $INITIAL_DANGLING"
log "Damping          : $DAMPING"
log "Threshold        : $THRESHOLD"
log "Max iterations   : $MAX_ITERATIONS"
log "Reducers         : $NUM_REDUCERS"
log "Streaming JAR    : $STREAMING_JAR"

# -----------------------------------------------------------------------------
# Clean local results
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

hdfs dfs -ls / >/dev/null 2>&1 \
    || fail "HDFS is unavailable"

log "Cleaning old HDFS output"

hdfs dfs -rm -r -f "$HDFS_ROOT" \
    >>"$LOG_FILE" 2>&1 || true

hdfs dfs -mkdir -p "$HDFS_INPUT"

log "Uploading dataset to HDFS"

hdfs dfs \
    -Ddfs.blocksize="$HDFS_BLOCK_SIZE" \
    -put -f "$DATASET" "$HDFS_INPUT/graph.txt" \
    >>"$LOG_FILE" 2>&1

hdfs dfs -setrep -w "$HDFS_REPLICATION" \
    "$HDFS_INPUT/graph.txt" \
    >>"$LOG_FILE" 2>&1

# -----------------------------------------------------------------------------
# Iterations
# -----------------------------------------------------------------------------

CURRENT_INPUT="$HDFS_INPUT/graph.txt"
DANGLING="$INITIAL_DANGLING"

TOTAL_START="$(date +%s)"
FINAL_ITERATION=0
CONVERGED=0
LOCAL_CURRENT=""

for ((iteration = 1; iteration <= MAX_ITERATIONS; iteration++)); do
    log "Starting iteration $iteration"

    CURRENT_OUTPUT="$HDFS_OUTPUT/iter$iteration"
    LOCAL_CURRENT="$OUTPUT_DIR/iter$iteration.txt"

    hdfs dfs -rm -r -f "$CURRENT_OUTPUT" \
        >>"$LOG_FILE" 2>&1 || true

    ITERATION_START="$(date +%s)"

    hadoop jar "$STREAMING_JAR" \
        -D "mapreduce.job.name=PageRank-Python-Iteration-$iteration" \
        -D "mapreduce.job.reduces=$NUM_REDUCERS" \
        -D "mapreduce.output.fileoutputformat.compress=false" \
        -files "$MAPPER,$REDUCER" \
        -cmdenv "NUM_NODES=$NUM_NODES" \
        -cmdenv "DAMPING=$DAMPING" \
        -cmdenv "DANGLING=$DANGLING" \
        -mapper mapper.py \
        -reducer reducer.py \
        -input "$CURRENT_INPUT" \
        -output "$CURRENT_OUTPUT" \
        2>&1 | tee -a "$LOG_FILE"

    ITERATION_END="$(date +%s)"
    ITERATION_SECONDS=$((ITERATION_END - ITERATION_START))

    echo "$iteration,$ITERATION_SECONDS" \
        >> "$BENCHMARK_DIR/time.csv"

    # Combine all reducer parts into one local result.
    hdfs dfs -cat "$CURRENT_OUTPUT"/part-* \
        > "$LOCAL_CURRENT"

    if (( iteration == 1 )); then
        python3 "$VERIFY" \
            --current "$LOCAL_CURRENT" \
            --iteration "$iteration" \
            --threshold "$THRESHOLD" \
            --benchmark-dir "$BENCHMARK_DIR" \
            2>&1 | tee -a "$LOG_FILE"
    else
        LOCAL_PREVIOUS="$OUTPUT_DIR/iter$((iteration - 1)).txt"

        python3 "$VERIFY" \
            --current "$LOCAL_CURRENT" \
            --previous "$LOCAL_PREVIOUS" \
            --iteration "$iteration" \
            --threshold "$THRESHOLD" \
            --benchmark-dir "$BENCHMARK_DIR" \
            2>&1 | tee -a "$LOG_FILE"
    fi

    DANGLING="$(tr -d '[:space:]' \
        < "$BENCHMARK_DIR/dangling.txt")"

    CONVERGED="$(tr -d '[:space:]' \
        < "$BENCHMARK_DIR/converged.txt")"

    FINAL_ITERATION="$iteration"

    log "Iteration time    : $ITERATION_SECONDS seconds"
    log "Next dangling    : $DANGLING"

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
TOTAL_SECONDS=$((TOTAL_END - TOTAL_START))

[[ -n "$LOCAL_CURRENT" && -f "$LOCAL_CURRENT" ]] \
    || fail "No final output was generated"

cp "$LOCAL_CURRENT" "$OUTPUT_DIR/final.txt"

FINAL_TOTAL_RANK="$(
    awk -F '\t' '{sum += $2} END {printf "%.15f", sum}' \
        "$OUTPUT_DIR/final.txt"
)"

{
    echo "Implementation=Python Hadoop Streaming"
    echo "Dataset=$DATASET"
    echo "Nodes=$NUM_NODES"
    echo "Damping=$DAMPING"
    echo "Threshold=$THRESHOLD"
    echo "Reducers=$NUM_REDUCERS"
    echo "Iterations=$FINAL_ITERATION"
    echo "Converged=$CONVERGED"
    echo "TotalPageRank=$FINAL_TOTAL_RANK"
    echo "TotalTimeSeconds=$TOTAL_SECONDS"
    echo "FinalOutput=$OUTPUT_DIR/final.txt"
    echo "LogFile=$LOG_FILE"
} > "$BENCHMARK_DIR/summary.txt"

log "PageRank completed"
log "Iterations       : $FINAL_ITERATION"
log "Converged        : $CONVERGED"
log "Total PageRank   : $FINAL_TOTAL_RANK"
log "Total time       : $TOTAL_SECONDS seconds"
log "Final output     : $OUTPUT_DIR/final.txt"
log "Benchmark        : $BENCHMARK_DIR"
log "Log              : $LOG_FILE"