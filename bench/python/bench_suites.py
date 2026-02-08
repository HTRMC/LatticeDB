"""Benchmark suites mirroring the Zig bench suite, but over the network."""

from bench_runner import BenchmarkResult, exec_query, run_benchmark
from gp_client import GrapheneDBClient


# ── Helpers ──────────────────────────────────────────────────────────────

async def setup_bench_data(client: GrapheneDBClient, scale: int) -> None:
    """Create and populate bench_data table (1000 * scale rows)."""
    await exec_query(client, "CREATE TABLE bench_data (id INT, val INT, category INT, label VARCHAR(100), amount FLOAT)")
    n = 1000 * scale
    for i in range(n):
        val = (i * 7 + 13) % 1000
        category = i % 10
        amount = (i * 3 + 7) % 500
        await exec_query(
            client,
            f"INSERT INTO bench_data VALUES ({i}, {val}, {category}, 'item_{i}', {amount}.0)"
        )


async def setup_bench_lookup(client: GrapheneDBClient, scale: int) -> None:
    """Create and populate bench_lookup table (10 * scale rows)."""
    await exec_query(client, "CREATE TABLE bench_lookup (id INT, name VARCHAR(50), region INT)")
    n = 10 * scale
    for i in range(n):
        region = i % 3
        await exec_query(client, f"INSERT INTO bench_lookup VALUES ({i}, 'lookup_{i}', {region})")


async def cleanup(client: GrapheneDBClient, *tables: str) -> None:
    """Drop tables, ignoring errors."""
    for t in tables:
        try:
            await exec_query(client, f"DROP TABLE {t}")
        except Exception:
            pass


# ── CRUD Suite ───────────────────────────────────────────────────────────

async def run_crud(client: GrapheneDBClient, scale: int) -> list[BenchmarkResult]:
    results = []
    await cleanup(client, "bench_data")

    # bulk_insert — setup + timed insert
    async def bulk_insert(c, s):
        n = 1000 * s
        for i in range(n):
            val = (i * 7 + 13) % 1000
            cat = i % 10
            amt = (i * 3 + 7) % 500
            await exec_query(c, f"INSERT INTO bench_data VALUES ({i}, {val}, {cat}, 'item_{i}', {amt}.0)")

    # Create table once for bulk_insert
    await exec_query(client, "CREATE TABLE bench_data (id INT, val INT, category INT, label VARCHAR(100), amount FLOAT)")
    results.append(await run_benchmark("bulk_insert", client, bulk_insert, warmup=0, iters=1, scale=scale))

    # point_query_seqscan
    async def point_query_seqscan(c, s):
        n = 100 * s
        total_rows = 1000 * s
        for i in range(n):
            id_ = (i * 17 + 5) % total_rows
            await exec_query(c, f"SELECT * FROM bench_data WHERE id = {id_}")

    results.append(await run_benchmark("point_query_seqscan", client, point_query_seqscan, scale=scale))

    # full_scan
    async def full_scan(c, s):
        await exec_query(c, "SELECT * FROM bench_data")

    results.append(await run_benchmark("full_scan", client, full_scan, scale=scale))

    # range_query_seqscan
    async def range_query_seqscan(c, s):
        await exec_query(c, "SELECT * FROM bench_data WHERE val BETWEEN 100 AND 200")

    results.append(await run_benchmark("range_query_seqscan", client, range_query_seqscan, scale=scale))

    # Create indexes for indexed benchmarks
    await exec_query(client, "CREATE INDEX idx_bench_id ON bench_data (id)")
    await exec_query(client, "CREATE INDEX idx_bench_val ON bench_data (val)")

    # point_query_indexscan
    async def point_query_indexscan(c, s):
        n = 100 * s
        total_rows = 1000 * s
        for i in range(n):
            id_ = (i * 17 + 5) % total_rows
            await exec_query(c, f"SELECT * FROM bench_data WHERE id = {id_}")

    results.append(await run_benchmark("point_query_indexscan", client, point_query_indexscan, scale=scale))

    # range_query_indexscan
    async def range_query_indexscan(c, s):
        await exec_query(c, "SELECT * FROM bench_data WHERE val BETWEEN 100 AND 200")

    results.append(await run_benchmark("range_query_indexscan", client, range_query_indexscan, scale=scale))

    # update
    async def update_bench(c, s):
        await exec_query(c, "UPDATE bench_data SET val = 999 WHERE category = 5")

    results.append(await run_benchmark("update", client, update_bench, warmup=0, iters=1, scale=scale))

    # delete
    async def delete_bench(c, s):
        await exec_query(c, "DELETE FROM bench_data WHERE category = 9")

    results.append(await run_benchmark("delete", client, delete_bench, warmup=0, iters=1, scale=scale))

    await cleanup(client, "bench_data")
    return results


# ── Analytical Suite ─────────────────────────────────────────────────────

async def run_analytical(client: GrapheneDBClient, scale: int) -> list[BenchmarkResult]:
    results = []
    await cleanup(client, "bench_data", "bench_lookup")
    await setup_bench_data(client, scale)
    await setup_bench_lookup(client, scale)

    # group_by_count
    async def group_by_count(c, s):
        await exec_query(c, "SELECT category, COUNT(*) FROM bench_data GROUP BY category")

    results.append(await run_benchmark("group_by_count", client, group_by_count, warmup=0, iters=1, scale=scale))

    # group_by_sum_avg
    async def group_by_sum_avg(c, s):
        await exec_query(c, "SELECT category, SUM(amount), AVG(val) FROM bench_data GROUP BY category")

    results.append(await run_benchmark("group_by_sum_avg", client, group_by_sum_avg, scale=scale))

    # join
    async def join_bench(c, s):
        await exec_query(c, "SELECT bench_data.id, bench_lookup.name FROM bench_data JOIN bench_lookup ON bench_data.category = bench_lookup.id")

    results.append(await run_benchmark("join", client, join_bench, scale=scale))

    # left_join
    async def left_join_bench(c, s):
        await exec_query(c, "SELECT bench_data.id, bench_lookup.name FROM bench_data LEFT JOIN bench_lookup ON bench_data.category = bench_lookup.id")

    results.append(await run_benchmark("left_join", client, left_join_bench, scale=scale))

    # order_by_limit
    async def order_by_limit(c, s):
        await exec_query(c, "SELECT * FROM bench_data ORDER BY val LIMIT 100")

    results.append(await run_benchmark("order_by_limit", client, order_by_limit, scale=scale))

    # min_max
    async def min_max(c, s):
        await exec_query(c, "SELECT MIN(val), MAX(val) FROM bench_data")

    results.append(await run_benchmark("min_max", client, min_max, scale=scale))

    await cleanup(client, "bench_data", "bench_lookup")
    return results


# ── Index Suite ──────────────────────────────────────────────────────────

async def run_index(client: GrapheneDBClient, scale: int) -> list[BenchmarkResult]:
    results = []
    await cleanup(client, "bench_data")
    await setup_bench_data(client, scale)

    # create_index
    async def create_index(c, s):
        await exec_query(c, "CREATE INDEX idx_bench_id ON bench_data (id)")

    results.append(await run_benchmark("create_index", client, create_index, warmup=0, iters=1, scale=scale))
    await exec_query(client, "DROP INDEX idx_bench_id")

    # unique_index
    async def unique_index(c, s):
        await exec_query(c, "CREATE UNIQUE INDEX idx_bench_id_unique ON bench_data (id)")

    results.append(await run_benchmark("unique_index", client, unique_index, warmup=0, iters=1, scale=scale))

    # point_query_no_index (val column, no index yet)
    async def point_query_val(c, s):
        n = 100 * s
        for i in range(n):
            val = (i * 13 + 7) % 1000
            await exec_query(c, f"SELECT * FROM bench_data WHERE val = {val}")

    results.append(await run_benchmark("point_query_no_index", client, point_query_val, warmup=1, iters=5, scale=scale))

    # Create index on val, then measure with index
    await exec_query(client, "CREATE INDEX idx_bench_val ON bench_data (val)")

    results.append(await run_benchmark("point_query_with_index", client, point_query_val, warmup=1, iters=5, scale=scale))

    # Report speedup
    no_idx = results[-2]
    with_idx = results[-1]
    if with_idx.mean_ns > 0:
        speedup = no_idx.mean_ns / with_idx.mean_ns
        print(f"  Index speedup ratio: {speedup:.2f}x")

    await cleanup(client, "bench_data")
    return results


# ── Transaction Suite ────────────────────────────────────────────────────

async def run_txn(client: GrapheneDBClient, scale: int) -> list[BenchmarkResult]:
    results = []
    await cleanup(client, "bench_txn")
    await exec_query(client, "CREATE TABLE bench_txn (id INT, counter INT)")

    # txn_commit_throughput
    async def txn_throughput(c, s):
        n = 100 * s
        for i in range(n):
            await exec_query(c, "BEGIN")
            await exec_query(c, f"INSERT INTO bench_txn VALUES ({i + 10000}, {i})")
            await exec_query(c, "COMMIT")

    results.append(await run_benchmark("txn_commit_throughput", client, txn_throughput, warmup=1, iters=5, scale=scale))

    # txn_rollback_overhead
    async def rollback_overhead(c, s):
        n = 100 * s
        for i in range(n):
            await exec_query(c, "BEGIN")
            await exec_query(c, f"INSERT INTO bench_txn VALUES ({i + 90000}, {i})")
            await exec_query(c, "ROLLBACK")

    results.append(await run_benchmark("txn_rollback_overhead", client, rollback_overhead, warmup=1, iters=5, scale=scale))

    # mixed_readwrite
    async def mixed_readwrite(c, s):
        n = 100 * s
        for i in range(n):
            await exec_query(c, f"INSERT INTO bench_txn VALUES ({i + 50000}, {i})")
            read_id = (i * 7 + 3) % (i + 1) + 50000
            await exec_query(c, f"SELECT * FROM bench_txn WHERE id = {read_id}")

    results.append(await run_benchmark("mixed_readwrite", client, mixed_readwrite, warmup=1, iters=5, scale=scale))

    await cleanup(client, "bench_txn")
    return results
