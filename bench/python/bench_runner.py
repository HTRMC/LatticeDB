"""Benchmark harness and CLI for GrapheneDB network benchmarks."""

import argparse
import asyncio
import csv
import statistics
import sys
import time
from dataclasses import dataclass

from gp_client import GrapheneDBClient
from gp_protocol import MSG_ERROR_RESPONSE


@dataclass
class BenchmarkResult:
    name: str
    total_ns: int
    min_ns: int
    max_ns: int
    mean_ns: int
    median_ns: int
    p95_ns: int
    p99_ns: int
    ops_count: int
    ops_per_sec: float


async def run_benchmark(
    name: str,
    client: GrapheneDBClient,
    func,
    warmup: int = 3,
    iters: int = 10,
    scale: int = 1,
) -> BenchmarkResult:
    """Run a benchmark function with warmup and measurement passes."""
    # Warmup
    for _ in range(warmup):
        await func(client, scale)

    # Measure
    latencies = []
    for _ in range(iters):
        start = time.perf_counter_ns()
        await func(client, scale)
        elapsed = time.perf_counter_ns() - start
        latencies.append(elapsed)

    latencies.sort()
    total = sum(latencies)
    mean = total // len(latencies)
    median = latencies[len(latencies) // 2]
    p95 = latencies[min(len(latencies) - 1, len(latencies) * 95 // 100)]
    p99 = latencies[min(len(latencies) - 1, len(latencies) * 99 // 100)]

    mean_secs = mean / 1_000_000_000
    ops_per_sec = 1.0 / mean_secs if mean_secs > 0 else 0

    return BenchmarkResult(
        name=name,
        total_ns=total,
        min_ns=latencies[0],
        max_ns=latencies[-1],
        mean_ns=mean,
        median_ns=median,
        p95_ns=p95,
        p99_ns=p99,
        ops_count=len(latencies),
        ops_per_sec=ops_per_sec,
    )


def fmt_ns(ns: int) -> str:
    """Format nanoseconds into human-readable time string."""
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.2f}s"
    elif ns >= 1_000_000:
        return f"{ns / 1_000_000:.2f}ms"
    elif ns >= 1_000:
        return f"{ns / 1_000:.2f}us"
    else:
        return f"{ns}ns"


def print_summary(results: list[BenchmarkResult]) -> None:
    """Print results in tabular format matching the Zig bench output."""
    print()
    print("=" * 96)
    print(f" {'Benchmark':<30} {'Mean':>12} {'Median':>12} {'P95':>12} {'Min':>12} {'Ops/sec':>12}")
    print("-" * 96)

    for r in results:
        print(
            f" {r.name:<30} {fmt_ns(r.mean_ns):>12} {fmt_ns(r.median_ns):>12} "
            f"{fmt_ns(r.p95_ns):>12} {fmt_ns(r.min_ns):>12} {r.ops_per_sec:>12.1f}"
        )

    print("=" * 96)
    print()


def write_csv(path: str, results: list[BenchmarkResult]) -> None:
    """Write results to CSV matching the Zig format."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "benchmark", "total_ns", "min_ns", "max_ns", "mean_ns",
            "median_ns", "p95_ns", "p99_ns", "ops_count", "ops_per_sec",
        ])
        for r in results:
            writer.writerow([
                r.name, r.total_ns, r.min_ns, r.max_ns, r.mean_ns,
                r.median_ns, r.p95_ns, r.p99_ns, r.ops_count, f"{r.ops_per_sec:.2f}",
            ])


async def exec_query(client: GrapheneDBClient, sql: str) -> None:
    """Execute a query, raise on error response."""
    msg = await client.query(sql)
    if msg.is_error:
        raise RuntimeError(f"SQL error: {msg.text} (sql: {sql})")


async def main():
    parser = argparse.ArgumentParser(description="GrapheneDB Network Benchmark")
    parser.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=4321, help="Server port (default: 4321)")
    parser.add_argument("--scale", type=int, default=1, help="Scale factor for row counts (default: 1)")
    parser.add_argument("--suite", default="all", choices=["crud", "analytical", "index", "txn", "all"],
                        help="Benchmark suite to run (default: all)")
    parser.add_argument("--output", default="bench/results/python_network_results.csv",
                        help="CSV output path")
    args = parser.parse_args()

    # Import suites here to avoid circular imports
    from bench_suites import run_crud, run_analytical, run_index, run_txn

    suites = {
        "crud": run_crud,
        "analytical": run_analytical,
        "index": run_index,
        "txn": run_txn,
    }

    client = GrapheneDBClient()
    print(f"Connecting to {args.host}:{args.port}...")
    await client.connect(args.host, args.port)
    print("Connected.\n")

    all_results: list[BenchmarkResult] = []

    try:
        if args.suite == "all":
            for name, suite_fn in suites.items():
                print(f"--- Running {name} suite ---")
                results = await suite_fn(client, args.scale)
                all_results.extend(results)
                print_summary(results)
        else:
            print(f"--- Running {args.suite} suite ---")
            results = await suites[args.suite](client, args.scale)
            all_results.extend(results)

        print_summary(all_results)
        write_csv(args.output, all_results)
        print(f"Results written to {args.output}")

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
