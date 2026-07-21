#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess

# ===========================
# Cấu hình
# ===========================

INPUT_FILE = ".\\dataset\\test.txt"

MAPPER = "mapper.py"
REDUCER = "reducer.py"

NUM_NODES = 4
DAMPING = 0.85
ITERATIONS = 10

# ===========================

env = os.environ.copy()
env["NUM_NODES"] = str(NUM_NODES)
env["DAMPING"] = str(DAMPING)

current_file = INPUT_FILE

print("=" * 60)
print("PageRank Simulation")
print("=" * 60)

for i in range(1, ITERATIONS + 1):

    print(f"\nIteration {i}")

    # -----------------------------
    # Đọc dữ liệu đầu vào
    # -----------------------------

    with open(current_file, "r", encoding="utf-8") as f:
        input_data = f.read()

    # -----------------------------
    # Mapper
    # -----------------------------

    mapper = subprocess.run(
        ["python", MAPPER],
        input=input_data,
        capture_output=True,
        text=True
    )

    mapper_output = mapper.stdout.splitlines()

    # -----------------------------
    # Shuffle & Sort
    # -----------------------------

    mapper_output.sort()

    shuffle_data = "\n".join(mapper_output)

    # -----------------------------
    # Reducer
    # -----------------------------

    reducer = subprocess.run(
        ["python", REDUCER],
        input=shuffle_data,
        capture_output=True,
        text=True,
        env=env
    )

    output = reducer.stdout

    output_file = f".\\output\iter{i}.txt"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(output)

    print(output)

    current_file = output_file

print("=" * 60)
print("Finished")
print("=" * 60)