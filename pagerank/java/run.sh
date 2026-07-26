#!/usr/bin/env bash
# Chay PageRank Java-Hadoop tren 1 bo dataset, tuan theo quy trinh trong
# PageRank_Hadoop_Project_Setup_Guide.txt muc 7 (Automation Script):
#   1. Clean old output  2. Upload input  3. Execute PageRank iterations
#   4. Save execution time  5. Download final output  6. Save logs
#
# Cach dung:
#   ./run.sh <file_canh_local> <ten_dataset> [damping=0.85] [tolerance=1e-6] [maxIter=20]
#
# Vi du:
#   ./run.sh ../dataset/toy.tsv toy
#   ./run.sh ../dataset/Wiki-Vote.txt wikivote
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Cach dung: $0 <file_canh_local> <ten_dataset> [damping=0.85] [tolerance=1e-6] [maxIter=20]"
  exit 1
fi

LOCAL_INPUT="$1"
DATASET_LABEL="$2"
DAMPING="${3:-0.85}"
TOLERANCE="${4:-1e-6}"
MAX_ITER="${5:-20}"

JAR="pagerank-hadoop.jar"
HDFS_INPUT_DIR="/pagerank/java/input"
HDFS_OUTPUT_BASE="/pagerank/java/output_${DATASET_LABEL}"
LOCAL_OUTPUT_DIR="../output/java_${DATASET_LABEL}"
LOCAL_LOG_DIR="../log"
BENCHMARK_FILE="../benchmark/results.csv"

mkdir -p "$LOCAL_OUTPUT_DIR" "$LOCAL_LOG_DIR" "../benchmark"

[ -f "$JAR" ] || { echo "Chua co $JAR - chay ./build.sh truoc."; exit 1; }
[ -f "$LOCAL_INPUT" ] || { echo "Khong tim thay file dataset: $LOCAL_INPUT"; exit 1; }

# 1. Clean old output (ca HDFS lan local, tranh loi thu muc da ton tai)
echo ">> [1/6] Don dep output cu"
hdfs dfs -rm -r -f -skipTrash "$HDFS_OUTPUT_BASE" >/dev/null 2>&1 || true
rm -rf "$LOCAL_OUTPUT_DIR"
mkdir -p "$LOCAL_OUTPUT_DIR"

# 2. Upload input
echo ">> [2/6] Upload dataset len HDFS: $HDFS_INPUT_DIR/${DATASET_LABEL}.tsv"
hdfs dfs -mkdir -p "$HDFS_INPUT_DIR"
hdfs dfs -put -f "$LOCAL_INPUT" "$HDFS_INPUT_DIR/${DATASET_LABEL}.tsv"

# 3 + 4. Execute PageRank iterations, driver tu in thoi gian thuc thi (CSV_RESULT)
LOG_FILE="$LOCAL_LOG_DIR/run_${DATASET_LABEL}_$(date +%Y%m%d_%H%M%S).log"
echo ">> [3/6][4/6] Chay PageRankDriver (dang ghi log: $LOG_FILE)"
hadoop jar "$JAR" pagerank.hadoop.PageRankDriver \
  "$HDFS_INPUT_DIR/${DATASET_LABEL}.tsv" \
  "$HDFS_OUTPUT_BASE" \
  "$DAMPING" "$TOLERANCE" "$MAX_ITER" "$DATASET_LABEL" \
  2>&1 | tee "$LOG_FILE"

# 5. Download final output (thu muc iterN cuoi cung, N = so vong lap thuc te)
FINAL_ITER_DIR=$(hdfs dfs -ls "$HDFS_OUTPUT_BASE" 2>/dev/null | awk '{print $NF}' \
  | grep '/iter[0-9]*$' | awk -F'iter' '{print $2"\t"$0}' | sort -n | tail -1 | cut -f2)

if [ -z "$FINAL_ITER_DIR" ]; then
  echo "!! Khong tim thay thu muc output cuoi cung tren HDFS, kiem tra log: $LOG_FILE"
  exit 1
fi

echo ">> [5/6] Tai toan bo output tung vong lap ve local: $LOCAL_OUTPUT_DIR"
hdfs dfs -get "$HDFS_OUTPUT_BASE"/* "$LOCAL_OUTPUT_DIR/"
echo ">> Ket qua vong lap cuoi cung nam trong: $LOCAL_OUTPUT_DIR/$(basename "$FINAL_ITER_DIR")"

# 6. Save logs + ghi vao bang benchmark chung
CSV_LINE=$(grep "^CSV_RESULT" "$LOG_FILE" || true)
if [ -n "$CSV_LINE" ]; then
  [ -f "$BENCHMARK_FILE" ] || echo "engine,dataset,num_nodes,iterations,seconds" > "$BENCHMARK_FILE"
  echo "$CSV_LINE" | sed 's/^CSV_RESULT,//' >> "$BENCHMARK_FILE"
  echo ">> [6/6] Da ghi ket qua vao $BENCHMARK_FILE"
else
  echo "!! Khong tim thay dong CSV_RESULT trong log - kiem tra $LOG_FILE de xem loi."
fi

echo ">> Hoan tat dataset '$DATASET_LABEL'."
echo "   Output: $LOCAL_OUTPUT_DIR"
echo "   Log:    $LOG_FILE"
echo "   Benchmark: $BENCHMARK_FILE"
