#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
===========================================================
PageRank Preprocessing
===========================================================

Convert an edge-list graph into adjacency-list format.

Input (edge list):
------------------
A B
A C
B D
D A

Output (adjacency list):
------------------------
A    B,C
B    D
C
D    A

Features
--------
- Ignore blank lines
- Ignore comment lines (#)
- Remove duplicate edges
- Automatically create dangling nodes
- Sort nodes alphabetically (or numerically if IDs are numbers)

Usage
-----
python3 preprocess.py input.txt output.txt
"""

import sys
from collections import defaultdict


def load_graph(input_file):
    """
    Read edge-list dataset.

    Returns
    -------
    graph : dict
        node -> set(out-neighbors)

    nodes : set
        all nodes in graph
    """

    graph = defaultdict(set)
    nodes = set()

    with open(input_file, "r", encoding="utf-8") as fin:

        for line in fin:

            line = line.strip()

            # Skip blank lines
            if not line:
                continue

            # Skip comments
            if line.startswith("#"):
                continue

            parts = line.split()

            if len(parts) != 2:
                continue

            src = parts[0]
            dst = parts[1]

            graph[src].add(dst)

            nodes.add(src)
            nodes.add(dst)

    return graph, nodes


def sort_nodes(nodes):
    """
    Sort node IDs.

    Numeric IDs:
        1 2 3 ...

    String IDs:
        A B C ...
    """

    try:
        return sorted(nodes, key=int)
    except ValueError:
        return sorted(nodes)


def save_graph(graph, nodes, output_file):
    """
    Save adjacency-list dataset.
    """

    with open(output_file, "w", encoding="utf-8") as fout:

        for node in sort_nodes(nodes):

            neighbors = graph.get(node, set())

            if neighbors:

                try:
                    neighbors = sorted(neighbors, key=int)
                except ValueError:
                    neighbors = sorted(neighbors)

                fout.write(
                    f"{node}\t{','.join(neighbors)}\n"
                )

            else:

                # Dangling node
                fout.write(f"{node}\t\n")


def print_statistics(graph, nodes):

    edge_count = sum(len(v) for v in graph.values())

    dangling = sum(
        1
        for node in nodes
        if len(graph.get(node, [])) == 0
    )

    print("=" * 60)
    print("PageRank Dataset Preprocessing")
    print("=" * 60)
    print(f"Total Nodes     : {len(nodes)}")
    print(f"Total Edges     : {edge_count}")
    print(f"Dangling Nodes  : {dangling}")
    print("=" * 60)


def main():

    if len(sys.argv) != 3:

        print("Usage:")
        print("python3 preprocess.py input.txt output.txt")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    graph, nodes = load_graph(input_file)

    save_graph(graph, nodes, output_file)

    print_statistics(graph, nodes)

    print(f"Output written to: {output_file}")


if __name__ == "__main__":
    main()