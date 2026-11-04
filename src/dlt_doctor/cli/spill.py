"""Handler for 'dlt-doctor spill' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.spill import SpillDoctor


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "spill", help="Calculate optimal spark.sql.shuffle.partitions to prevent disk spill"
    )
    p.add_argument(
        "--dataset-gb",
        "--shuffle-gb",
        dest="dataset_gb",
        type=float,
        required=True,
        help="Total dataset / shuffle read size in GB",
    )
    p.add_argument(
        "--cores",
        type=int,
        default=16,
        help="Total available cluster worker CPU cores (default: 16)",
    )


def handle(args: argparse.Namespace) -> int:
    doctor = SpillDoctor()
    advice = doctor.calculate_optimal_partitions(
        dataset_size_gb=args.dataset_gb,
        total_cluster_cores=args.cores,
    )

    print("=" * 70)
    print(" DLT-Doctor Shuffle Spill Prevention & Tuning (Calculator)")
    print("=" * 70)
    print(f"Dataset Size:              {advice.dataset_size_gb:.1f} GB")
    print(f"Optimal Partition Count:   {advice.optimal_partitions}")
    print(f"Target Partition Size:     {advice.target_partition_size_mb} MB")
    print(f"Spill Risk under Default:  {advice.has_spill_risk}")
    print(f"Rationale:                 {advice.rationale}")
    print("\nRecommended Spark Configurations:")
    for k, v in advice.recommended_configs.items():
        print(f"  spark.conf.set('{k}', '{v}')")
    print("-" * 70)
    return 0
