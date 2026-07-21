#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PageRank Reducer

Input:
    Node<TAB>value

value có thể là:

    LINKS:B,C,D

hoặc

    0.333333

Output

Node<TAB>NewRank<TAB>AdjacencyList
"""

import os
import sys

# ---------------------------------------------------------------------

DEFAULT_DAMPING = 0.85

DAMPING = float(
    os.getenv(
        "DAMPING",
        DEFAULT_DAMPING
    )
)

NUM_NODES = int(
    os.getenv(
        "NUM_NODES",
        "1"
    )
)

# ---------------------------------------------------------------------


def log(message):
    print(message, file=sys.stderr)


def compute_rank(rank_sum):
    """
    Compute new PageRank.
    """

    base = (1.0 - DAMPING) / NUM_NODES

    return base + DAMPING * rank_sum


def emit(node, rank_sum, links):
    """
    Emit one node.
    """

    rank = compute_rank(rank_sum)

    print(
        f"{node}\t{rank:.10f}\t{links}"
    )


# ---------------------------------------------------------------------

current_node = None

rank_sum = 0.0

graph = ""

# ---------------------------------------------------------------------

for line in sys.stdin:

    line = line.strip()

    if line == "":
        continue

    try:

        node, value = line.split("\t", 1)

    except ValueError:

        log(f"[WARN] Bad record: {line}")

        continue

    # ----------------------------------------

    if current_node is None:

        current_node = node

    # ----------------------------------------

    if node != current_node:

        emit(
            current_node,
            rank_sum,
            graph
        )

        current_node = node

        rank_sum = 0.0

        graph = ""

    # ----------------------------------------

    if value.startswith("LINKS:"):

        graph = value.replace(
            "LINKS:",
            ""
        )

    else:

        try:

            rank_sum += float(value)

        except ValueError:

            log(
                f"[WARN] Invalid contribution: {line}"
            )

# ---------------------------------------------------------------------

if current_node is not None:

    emit(
        current_node,
        rank_sum,
        graph
    )