#!/usr/bin/env bash
# Bien dich va dong goi PageRank Java-Hadoop thanh 1 file jar.
# Chay tu thu muc pagerank/java/ (thu muc chua src/, build.sh, run.sh nay).
set -euo pipefail

SRC_DIR="src/main/java"
BUILD_DIR="build/classes"
JAR_NAME="pagerank-hadoop.jar"

command -v hadoop >/dev/null 2>&1 || {
  echo "Khong tim thay lenh 'hadoop'. Kiem tra bien moi truong PATH / HADOOP_HOME"
  echo "(chay: source ~/.bashrc  hoac mo lai terminal)."
  exit 1
}

echo ">> Don dep build cu"
rm -rf "$BUILD_DIR" "$JAR_NAME"
mkdir -p "$BUILD_DIR"

echo ">> Bien dich Java (dung classpath cua Hadoop dang cai, --release 11 de tuong thich JVM cua Hadoop)"
javac --release 11 -classpath "$(hadoop classpath)" -d "$BUILD_DIR" $(find "$SRC_DIR" -name "*.java")

echo ">> Dong goi jar"
jar -cf "$JAR_NAME" -C "$BUILD_DIR" .

echo ">> Xong: $(pwd)/$JAR_NAME"
