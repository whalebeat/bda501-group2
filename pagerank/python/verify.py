#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Validate one PageRank iteration.

Functions:
- Calculate total PageRank.
- Calculate dangling mass.
- Compare two consecutive iterations.
- Calculate maximum and average delta.
- Check convergence.
- Write top20.csv and convergence.csv.

Example:

First iteration:
    python3 verify.py \
        --current output/iter1.txt \
        --iteration 1

Later iteration:
    python3 verify.py \
        --current output/iter2.txt \
        --previous output/iter1.txt \
        --iteration 2
"""

import argparse
import csv
import math
import os
import sys
from typing import Dict, Tuple


RankRecord = Tuple[float, str]


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate a PageRank iteration."
    )

    parser.add_argument(
        "--current",
        required=True,
        help="Current iteration result file."
    )
    parser.add_argument(
        "--previous",
        help="Previous iteration result file."
    )
    parser.add_argument(
        "--iteration",
        required=True,
        type=int,
        help="Current iteration number."
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=1e-6,
        help="Convergence threshold."
    )
    parser.add_argument(
        "--benchmark-dir",
        help="Directory for benchmark files."
    )

    return parser.parse_args()


def read_result(filename: str) -> Dict[str, RankRecord]:
    """Read a PageRank result file."""
    ranks: Dict[str, RankRecord] = {}

    with open(filename, "r", encoding="utf-8") as input_file:
        for line_number, raw_line in enumerate(input_file, start=1):
            # Preserve the trailing tab representing an empty adjacency list.
            fields = raw_line.rstrip("\r\n").split("\t")

            if len(fields) != 3:
                print(
                    f"[WARN] Invalid record at {filename}:{line_number}",
                    file=sys.stderr
                )
                continue

            node = fields[0].strip()
            adjacency = fields[2].strip()

            try:
                rank = float(fields[1])
            except ValueError:
                print(
                    f"[WARN] Invalid rank at {filename}:{line_number}",
                    file=sys.stderr
                )
                continue

            if node:
                ranks[node] = (rank, adjacency)

    if not ranks:
        raise RuntimeError(f"No valid PageRank records found in {filename}")

    return ranks


def calculate_metrics(
    current: Dict[str, RankRecord],
    previous: Dict[str, RankRecord] | None
) -> tuple[float, float, float, float]:
    """Calculate total rank, dangling mass, max delta and average delta."""
    total_rank = sum(rank for rank, _ in current.values())

    dangling_mass = sum(
        rank
        for rank, adjacency in current.values()
        if adjacency == ""
    )

    if previous is None:
        return total_rank, dangling_mass, math.inf, math.inf

    all_nodes = set(current) | set(previous)
    deltas = []

    for node in all_nodes:
        current_rank = current.get(node, (0.0, ""))[0]
        previous_rank = previous.get(node, (0.0, ""))[0]
        deltas.append(abs(current_rank - previous_rank))

    max_delta = max(deltas, default=0.0)
    average_delta = sum(deltas) / len(deltas) if deltas else 0.0

    return total_rank, dangling_mass, max_delta, average_delta


def write_top20(
    current: Dict[str, RankRecord],
    benchmark_dir: str
) -> None:
    """Write the 20 highest-ranked nodes."""
    top20 = sorted(
        current.items(),
        key=lambda item: item[1][0],
        reverse=True
    )[:20]

    output_file = os.path.join(benchmark_dir, "top20.csv")

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Node", "PageRank"])

        for node, (rank, _) in top20:
            writer.writerow([node, f"{rank:.15f}"])


def append_convergence(
    benchmark_dir: str,
    iteration: int,
    total_rank: float,
    dangling_mass: float,
    max_delta: float,
    average_delta: float,
    converged: bool
) -> None:
    """Append validation metrics to convergence.csv."""
    output_file = os.path.join(benchmark_dir, "convergence.csv")
    file_exists = os.path.exists(output_file)

    with open(output_file, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow([
                "Iteration",
                "TotalPageRank",
                "DanglingMass",
                "MaxDelta",
                "AverageDelta",
                "Converged"
            ])

        writer.writerow([
            iteration,
            f"{total_rank:.15f}",
            f"{dangling_mass:.15f}",
            "inf" if math.isinf(max_delta) else f"{max_delta:.15e}",
            "inf" if math.isinf(average_delta)
            else f"{average_delta:.15e}",
            "YES" if converged else "NO"
        ])


def main() -> None:
    """Validate the current PageRank output."""
    args = parse_arguments()

    if args.iteration <= 0:
        raise ValueError("--iteration must be greater than zero")

    if args.threshold <= 0:
        raise ValueError("--threshold must be greater than zero")

    project_root = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )

    benchmark_dir = args.benchmark_dir or os.path.join(
        project_root,
        "benchmark"
    )
    os.makedirs(benchmark_dir, exist_ok=True)

    current = read_result(args.current)
    previous = read_result(args.previous) if args.previous else None

    (
        total_rank,
        dangling_mass,
        max_delta,
        average_delta
    ) = calculate_metrics(current, previous)

    converged = (
        previous is not None
        and max_delta < args.threshold
    )

    with open(
        os.path.join(benchmark_dir, "dangling.txt"),
        "w",
        encoding="utf-8"
    ) as file:
        file.write(f"{dangling_mass:.15f}\n")

    with open(
        os.path.join(benchmark_dir, "convergence.txt"),
        "w",
        encoding="utf-8"
    ) as file:
        file.write(
            "inf\n" if math.isinf(max_delta)
            else f"{max_delta:.15e}\n"
        )

    with open(
        os.path.join(benchmark_dir, "converged.txt"),
        "w",
        encoding="utf-8"
    ) as file:
        file.write("1\n" if converged else "0\n")

    write_top20(current, benchmark_dir)

    append_convergence(
        benchmark_dir=benchmark_dir,
        iteration=args.iteration,
        total_rank=total_rank,
        dangling_mass=dangling_mass,
        max_delta=max_delta,
        average_delta=average_delta,
        converged=converged
    )

    probability_error = abs(total_rank - 1.0)

    print("=" * 60)
    print(f"Iteration       : {args.iteration}")
    print(f"Nodes           : {len(current)}")
    print(f"Total PageRank  : {total_rank:.15f}")
    print(f"Rank Error      : {probability_error:.3e}")
    print(f"Dangling Mass   : {dangling_mass:.15f}")

    if previous is None:
        print("Max Delta       : N/A")
        print("Average Delta   : N/A")
    else:
        print(f"Max Delta       : {max_delta:.15e}")
        print(f"Average Delta   : {average_delta:.15e}")

    print(f"Converged       : {'YES' if converged else 'NO'}")
    print("=" * 60)


if __name__ == "__main__":
    main()