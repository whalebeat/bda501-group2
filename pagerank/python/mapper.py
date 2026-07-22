#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PageRank Mapper for Hadoop Streaming.

Input iteration đầu:
    Node<TAB>AdjacencyList

Input các iteration tiếp:
    Node<TAB>PageRank<TAB>AdjacencyList

Output:
    Node<TAB>LINKS:AdjacencyList
    Neighbor<TAB>Contribution
"""

import os
import sys
from typing import Optional, Tuple, List


def log(message: str) -> None:
    """Write warning messages to stderr."""
    print(message, file=sys.stderr)


def get_num_nodes() -> int:
    """Read and validate NUM_NODES."""
    try:
        value = int(os.getenv("NUM_NODES", "0"))
    except ValueError as exc:
        raise RuntimeError("NUM_NODES must be an integer") from exc

    if value <= 0:
        raise RuntimeError("NUM_NODES must be greater than zero")

    return value


NUM_NODES = get_num_nodes()
INITIAL_RANK = 1.0 / NUM_NODES


def parse_line(line: str) -> Optional[Tuple[str, float, List[str]]]:
    """
    Parse an input record.

    Important: only remove newline characters, not tabs, because dangling
    nodes have an empty adjacency list after the final tab.
    """
    fields = line.rstrip("\r\n").split("\t")

    if len(fields) == 2:
        node = fields[0].strip()
        rank = INITIAL_RANK
        adjacency = fields[1].strip()

    elif len(fields) == 3:
        node = fields[0].strip()

        try:
            rank = float(fields[1])
        except ValueError:
            log(f"[WARN] Invalid PageRank: {line.rstrip()}")
            return None

        adjacency = fields[2].strip()

    else:
        log(f"[WARN] Invalid record: {line.rstrip()}")
        return None

    if not node:
        log(f"[WARN] Empty node ID: {line.rstrip()}")
        return None

    outlinks = [
        neighbor.strip()
        for neighbor in adjacency.split(",")
        if neighbor.strip()
    ]

    return node, rank, outlinks


def main() -> None:
    """Run the mapper."""
    for raw_line in sys.stdin:
        if not raw_line.strip():
            continue

        record = parse_line(raw_line)

        if record is None:
            continue

        node, rank, outlinks = record

        # Preserve graph structure for the next iteration.
        print(f"{node}\tLINKS:{','.join(outlinks)}")

        # A dangling node does not emit link contributions.
        if not outlinks:
            continue

        contribution = rank / len(outlinks)

        for neighbor in outlinks:
            print(f"{neighbor}\t{contribution:.15f}")


if __name__ == "__main__":
    main()