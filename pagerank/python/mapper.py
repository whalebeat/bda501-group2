#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PageRank Mapper
---------------

Input format:
    Node<TAB>Rank<TAB>Neighbor1,Neighbor2,...

Example:
    A    1.0    B,C,D

Output:
    1. Graph structure
        A    LINKS:B,C,D

    2. Rank contribution
        B    0.333333
        C    0.333333
        D    0.333333
"""

import sys

DEFAULT_RANK = 1.0

def log(message: str):
    """Write message to stderr."""
    print(message, file=sys.stderr)

def parse_line(line: str):
    """
    Parse one input record.

    Supported formats:

    1. Initial input
       Node<TAB>AdjacencyList

    2. Intermediate result
       Node<TAB>Rank<TAB>AdjacencyList
    """

    parts = line.strip().split("\t")

    # ------------------------------------------------------------------
    # Initial input
    # ------------------------------------------------------------------

    if len(parts) == 2:

        node = parts[0].strip()

        rank = DEFAULT_RANK

        links = parts[1].strip()

    # ------------------------------------------------------------------
    # Intermediate iterations
    # ------------------------------------------------------------------

    elif len(parts) == 3:

        node = parts[0].strip()

        try:
            rank = float(parts[1])
        except ValueError:
            log(f"[WARN] Invalid rank: {line.strip()}")
            return None

        links = parts[2].strip()

    # ------------------------------------------------------------------

    else:

        log(f"[WARN] Invalid record: {line.strip()}")
        return None

    if links == "":
        outlinks = []
    else:
        outlinks = [
            link.strip()
            for link in links.split(",")
            if link.strip()
        ]

    return node, rank, outlinks

def emit_graph(node, outlinks):
    """
    Preserve graph structure.
    """

    graph = ",".join(outlinks)

    print(f"{node}\tLINKS:{graph}")


def emit_contribution(rank, outlinks):
    """
    Emit PageRank contribution.
    """

    if len(outlinks) == 0:
        return

    contribution = rank / len(outlinks)

    for neighbor in outlinks:
        print(f"{neighbor}\t{contribution:.10f}")


def main():

    for raw in sys.stdin:

        raw = raw.strip()

        if raw == "":
            continue

        record = parse_line(raw)

        if record is None:
            continue

        node, rank, outlinks = record

        emit_graph(node, outlinks)

        emit_contribution(rank, outlinks)


if __name__ == "__main__":
    main()