#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PageRank Reducer for Hadoop Streaming.

Input:
    Node<TAB>LINKS:AdjacencyList
    Node<TAB>Contribution

Output:
    Node<TAB>NewPageRank<TAB>AdjacencyList

DANGLING is the dangling mass from the previous iteration.
For iteration 1, run.sh calculates it from the initial graph using rank 1/N.
"""

import os
import sys


def log(message: str) -> None:
    """Write warning messages to stderr."""
    print(message, file=sys.stderr)


def read_positive_int(name: str) -> int:
    """Read a positive integer environment variable."""
    try:
        value = int(os.getenv(name, "0"))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc

    if value <= 0:
        raise RuntimeError(f"{name} must be greater than zero")

    return value


def read_float(name: str, default: str) -> float:
    """Read a float environment variable."""
    try:
        return float(os.getenv(name, default))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be numeric") from exc


NUM_NODES = read_positive_int("NUM_NODES")
DAMPING = read_float("DAMPING", "0.85")
DANGLING = read_float("DANGLING", "0.0")

if not 0.0 < DAMPING < 1.0:
    raise RuntimeError("DAMPING must be between 0 and 1")

if DANGLING < 0.0:
    raise RuntimeError("DANGLING cannot be negative")

BASE_RANK = (1.0 - DAMPING) / NUM_NODES
DANGLING_SHARE = DAMPING * DANGLING / NUM_NODES


def emit(node: str, rank_sum: float, adjacency: str) -> None:
    """Calculate and emit the new PageRank of one node."""
    new_rank = BASE_RANK + DAMPING * rank_sum + DANGLING_SHARE
    print(f"{node}\t{new_rank:.15f}\t{adjacency}")


def main() -> None:
    """Run the reducer over sorted mapper output."""
    current_node = None
    rank_sum = 0.0
    adjacency = ""

    for raw_line in sys.stdin:
        line = raw_line.rstrip("\r\n")

        if not line:
            continue

        try:
            node, value = line.split("\t", 1)
        except ValueError:
            log(f"[WARN] Invalid mapper output: {line}")
            continue

        if current_node is None:
            current_node = node

        elif node != current_node:
            emit(current_node, rank_sum, adjacency)

            current_node = node
            rank_sum = 0.0
            adjacency = ""

        if value.startswith("LINKS:"):
            adjacency = value[len("LINKS:"):]

        else:
            try:
                rank_sum += float(value)
            except ValueError:
                log(f"[WARN] Invalid contribution: {line}")

    if current_node is not None:
        emit(current_node, rank_sum, adjacency)


if __name__ == "__main__":
    main()