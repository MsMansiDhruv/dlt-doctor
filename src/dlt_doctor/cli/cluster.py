"""Handler for 'dlt-doctor cluster' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.cluster import ClusterDoctor, ColumnProfile


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "cluster", help="Recommend optimal Liquid Clustering (CLUSTER BY) keys"
    )
    p.add_argument("--table", required=True, help="Target Delta table")
    p.add_argument(
        "--cols",
        nargs="+",
        required=True,
        help="Candidate columns as name or name:type:distinct_count:filter_freq",
    )
    p.add_argument("--rows", type=int, default=1_000_000, help="Estimated total table row count")


def handle(args: argparse.Namespace) -> int:
    profiles: list[ColumnProfile] = []
    for c_spec in args.cols:
        parts = c_spec.split(":")
        col_name = parts[0]
        data_type = parts[1] if len(parts) > 1 else "string"
        distinct_count = int(parts[2]) if len(parts) > 2 else 5000
        filter_freq = float(parts[3]) if len(parts) > 3 else 0.8
        profiles.append(
            ColumnProfile(
                name=col_name,
                data_type=data_type,
                distinct_count=distinct_count,
                filter_frequency_score=filter_freq,
            )
        )

    doctor = ClusterDoctor()
    rec = doctor.advise_clustering(args.table, profiles, total_rows=args.rows)

    print("=" * 70)
    print(" DLT-Doctor Liquid Clustering Advisor (Calculator & Advisory Template)")
    print(f" Target Table: {args.table} | Total Estimated Rows: {args.rows:,}")
    print("=" * 70)
    print(f"Recommended CLUSTER BY keys: {rec.recommended_keys}")
    print(f"Rationale:                   {rec.migration_advice}")
    print("\nDDL Definition:\n")
    print(rec.alter_cluster_ddl)
    print("-" * 70)
    return 0
