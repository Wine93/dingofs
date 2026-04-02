# SPDX-License-Identifier: Apache-2.0
#
# Performance benchmarks for DingoFS connector.
# Run with: pytest test_performance.py -xvs

# Standard
import os
import shutil
import tempfile
import time

# Third Party
import pytest

# Local
from dingofs_connector.native_engine import SYNC_ALWAYS, SYNC_NONE, NativeIOEngine


def _format_throughput(bytes_total: int, elapsed: float) -> str:
    """Format throughput as human-readable string."""
    if elapsed <= 0:
        return "inf"
    mb = bytes_total / (1024 * 1024)
    return f"{mb / elapsed:.1f} MB/s"


def _format_size(size: int) -> str:
    """Format byte size as human-readable."""
    if size >= 1024 * 1024:
        return f"{size // (1024 * 1024)}MB"
    elif size >= 1024:
        return f"{size // 1024}KB"
    return f"{size}B"


class TestWriteThroughput:
    """Benchmark write (SET) throughput."""

    @pytest.mark.parametrize(
        "chunk_size",
        [256 * 1024, 1024 * 1024, 4 * 1024 * 1024],
        ids=["256KB", "1MB", "4MB"],
    )
    def test_write_throughput(self, tmp_dir, chunk_size):
        """Measure write throughput for different chunk sizes."""
        num_workers = 8
        num_chunks = 32
        client = NativeIOEngine(tmp_dir, num_workers=num_workers)
        try:
            # Prepare data
            keys = [f"write_bench_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]

            # Warm up
            client.set_sync("warmup", memoryview(bytearray(chunk_size)))

            # Benchmark
            start = time.perf_counter()
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            print(
                f"\n  WRITE {_format_size(chunk_size)} x {num_chunks} chunks, "
                f"{num_workers} workers: {throughput} ({elapsed:.3f}s)"
            )
        finally:
            client.close()


class TestReadThroughput:
    """Benchmark read (GET) throughput."""

    @pytest.mark.parametrize(
        "chunk_size",
        [256 * 1024, 1024 * 1024, 4 * 1024 * 1024],
        ids=["256KB", "1MB", "4MB"],
    )
    def test_read_throughput(self, tmp_dir, chunk_size):
        """Measure read throughput for different chunk sizes."""
        num_workers = 8
        num_chunks = 32
        client = NativeIOEngine(tmp_dir, num_workers=num_workers)
        try:
            # Write data first
            keys = [f"read_bench_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])

            # Prepare read buffers
            read_bufs = [bytearray(chunk_size) for _ in range(num_chunks)]

            # Flush dirty pages to disk (does NOT drop page cache;
            # this is a warm-read benchmark)
            try:
                os.sync()
            except Exception:
                pass

            # Benchmark
            start = time.perf_counter()
            client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            print(
                f"\n  READ {_format_size(chunk_size)} x {num_chunks} chunks, "
                f"{num_workers} workers: {throughput} ({elapsed:.3f}s)"
            )

            # Verify data integrity
            for i in range(num_chunks):
                assert bufs[i] == read_bufs[i], f"Data mismatch at chunk {i}"
        finally:
            client.close()


class TestConcurrencyScaling:
    """Benchmark throughput scaling with different worker counts."""

    @pytest.mark.parametrize("num_workers", [1, 4, 8, 16], ids=["1w", "4w", "8w", "16w"])
    def test_write_scaling(self, num_workers):
        """Measure write throughput scaling with worker count."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        client = None
        try:
            chunk_size = 1024 * 1024  # 1 MB
            num_chunks = 32
            client = NativeIOEngine(tmp_dir, num_workers=num_workers)

            keys = [f"scale_w_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]

            start = time.perf_counter()
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            print(
                f"\n  WRITE 1MB x {num_chunks}, {num_workers} workers: "
                f"{throughput} ({elapsed:.3f}s)"
            )
        finally:
            if client is not None:
                client.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @pytest.mark.parametrize("num_workers", [1, 4, 8, 16], ids=["1w", "4w", "8w", "16w"])
    def test_read_scaling(self, num_workers):
        """Measure read throughput scaling with worker count."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        client = None
        try:
            chunk_size = 1024 * 1024  # 1 MB
            num_chunks = 32
            client = NativeIOEngine(tmp_dir, num_workers=num_workers)

            keys = [f"scale_r_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])

            read_bufs = [bytearray(chunk_size) for _ in range(num_chunks)]

            start = time.perf_counter()
            client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            print(
                f"\n  READ 1MB x {num_chunks}, {num_workers} workers: "
                f"{throughput} ({elapsed:.3f}s)"
            )
        finally:
            if client is not None:
                client.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)


class TestExistsThroughput:
    """Benchmark EXISTS operation throughput."""

    def test_exists_throughput(self, tmp_dir):
        """Measure EXISTS throughput."""
        num_workers = 8
        num_keys = 100
        client = NativeIOEngine(tmp_dir, num_workers=num_workers)
        try:
            # Write keys
            keys = [f"exists_bench_{i}" for i in range(num_keys)]
            chunk_size = 4096
            bufs = [bytearray(chunk_size) for _ in range(num_keys)]
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])

            # Benchmark EXISTS
            start = time.perf_counter()
            results = client.batch_exists_sync(keys)
            elapsed = time.perf_counter() - start

            assert all(results)
            ops_per_sec = num_keys / elapsed if elapsed > 0 else float("inf")
            print(
                f"\n  EXISTS {num_keys} keys, {num_workers} workers: "
                f"{ops_per_sec:.0f} ops/s ({elapsed:.3f}s)"
            )
        finally:
            client.close()


class TestSyncModeComparison:
    """Compare throughput between SYNC_NONE and SYNC_ALWAYS modes."""

    @pytest.mark.parametrize(
        "sync_mode",
        [SYNC_NONE, SYNC_ALWAYS],
        ids=["sync_none", "sync_always"],
    )
    @pytest.mark.parametrize(
        "chunk_size",
        [256 * 1024, 1024 * 1024, 4 * 1024 * 1024],
        ids=["256KB", "1MB", "4MB"],
    )
    def test_write_sync_mode_comparison(self, chunk_size, sync_mode):
        """Measure write throughput under different sync modes."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        client = None
        try:
            num_workers = 8
            num_chunks = 32
            client = NativeIOEngine(
                tmp_dir, num_workers=num_workers, sync_mode=sync_mode
            )

            keys = [f"sync_w_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]

            start = time.perf_counter()
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            mode_name = "SYNC_NONE" if sync_mode == SYNC_NONE else "SYNC_ALWAYS"
            print(
                f"\n  WRITE {_format_size(chunk_size)} x {num_chunks}, "
                f"sync_mode={mode_name}: {throughput} ({elapsed:.3f}s)"
            )
        finally:
            if client is not None:
                client.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @pytest.mark.parametrize(
        "sync_mode",
        [SYNC_NONE, SYNC_ALWAYS],
        ids=["sync_none", "sync_always"],
    )
    @pytest.mark.parametrize(
        "chunk_size",
        [256 * 1024, 1024 * 1024, 4 * 1024 * 1024],
        ids=["256KB", "1MB", "4MB"],
    )
    def test_read_sync_mode_comparison(self, chunk_size, sync_mode):
        """Measure read throughput under different sync modes."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        client = None
        try:
            num_workers = 8
            num_chunks = 32
            client = NativeIOEngine(
                tmp_dir, num_workers=num_workers, sync_mode=sync_mode
            )

            # Write data first
            keys = [f"sync_r_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])

            # Prepare read buffers
            read_bufs = [bytearray(chunk_size) for _ in range(num_chunks)]

            start = time.perf_counter()
            client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])
            elapsed = time.perf_counter() - start

            total_bytes = num_chunks * chunk_size
            throughput = _format_throughput(total_bytes, elapsed)
            mode_name = "SYNC_NONE" if sync_mode == SYNC_NONE else "SYNC_ALWAYS"
            print(
                f"\n  READ {_format_size(chunk_size)} x {num_chunks}, "
                f"sync_mode={mode_name}: {throughput} ({elapsed:.3f}s)"
            )

            # Verify data integrity
            for i in range(num_chunks):
                assert bufs[i] == read_bufs[i], f"Data mismatch at chunk {i}"
        finally:
            if client is not None:
                client.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)


class TestEndToEndBenchmark:
    """End-to-end benchmarks simulating real KV cache workloads."""

    def test_store_throughput(self):
        """Simulate KV cache store with multiple rounds."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        client = None
        try:
            chunk_size = 1024 * 1024  # 1 MB
            num_workers = 8
            num_rounds = 5
            num_chunks = 32
            client = NativeIOEngine(tmp_dir, num_workers=num_workers)

            round_throughputs = []
            for r in range(num_rounds):
                keys = [f"store_r{r}_{i}" for i in range(num_chunks)]
                bufs = [
                    bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)
                ]

                start = time.perf_counter()
                client.batch_set_sync(keys, [memoryview(b) for b in bufs])
                elapsed = time.perf_counter() - start

                total_bytes = num_chunks * chunk_size
                tp = _format_throughput(total_bytes, elapsed)
                round_throughputs.append((total_bytes, elapsed))
                print(f"\n  STORE round {r}: {tp} ({elapsed:.3f}s)")

            total_bytes_all = sum(t[0] for t in round_throughputs)
            total_elapsed_all = sum(t[1] for t in round_throughputs)
            avg_tp = _format_throughput(total_bytes_all, total_elapsed_all)
            print(f"\n  STORE Average: {avg_tp}")
        finally:
            if client is not None:
                client.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_retrieve_throughput(self):
        """Simulate KV cache retrieve (100% hit) with multiple rounds."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        client = None
        try:
            chunk_size = 1024 * 1024  # 1 MB
            num_workers = 8
            num_rounds = 5
            num_chunks = 32
            client = NativeIOEngine(tmp_dir, num_workers=num_workers)

            # Store data
            keys = [f"retrieve_{i}" for i in range(num_chunks)]
            bufs = [bytearray(os.urandom(chunk_size)) for _ in range(num_chunks)]
            client.batch_set_sync(keys, [memoryview(b) for b in bufs])

            round_throughputs = []
            for r in range(num_rounds):
                read_bufs = [bytearray(chunk_size) for _ in range(num_chunks)]

                start = time.perf_counter()
                client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])
                elapsed = time.perf_counter() - start

                total_bytes = num_chunks * chunk_size
                tp = _format_throughput(total_bytes, elapsed)
                round_throughputs.append((total_bytes, elapsed))
                print(f"\n  RETRIEVE round {r}: {tp} ({elapsed:.3f}s)")

                # Verify data integrity
                for i in range(num_chunks):
                    assert bufs[i] == read_bufs[i], (
                        f"Data mismatch at chunk {i}, round {r}"
                    )

            total_bytes_all = sum(t[0] for t in round_throughputs)
            total_elapsed_all = sum(t[1] for t in round_throughputs)
            avg_tp = _format_throughput(total_bytes_all, total_elapsed_all)
            print(f"\n  RETRIEVE Average: {avg_tp}")
        finally:
            if client is not None:
                client.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_mixed_workload(self):
        """Benchmark mixed read/write workload."""
        tmp_dir = tempfile.mkdtemp(prefix="dingofs_bench_")
        client = None
        try:
            chunk_size = 1024 * 1024  # 1 MB
            num_workers = 8
            num_rounds = 5
            num_base_keys = 20
            num_new_per_round = 10
            client = NativeIOEngine(tmp_dir, num_workers=num_workers)

            # Write base keys
            base_keys = [f"mixed_base_{i}" for i in range(num_base_keys)]
            base_bufs = [
                bytearray(os.urandom(chunk_size)) for _ in range(num_base_keys)
            ]
            client.batch_set_sync(
                base_keys, [memoryview(b) for b in base_bufs]
            )

            for r in range(num_rounds):
                # Write new keys
                new_keys = [
                    f"mixed_new_r{r}_{i}" for i in range(num_new_per_round)
                ]
                new_bufs = [
                    bytearray(os.urandom(chunk_size))
                    for _ in range(num_new_per_round)
                ]

                w_start = time.perf_counter()
                client.batch_set_sync(
                    new_keys, [memoryview(b) for b in new_bufs]
                )
                w_elapsed = time.perf_counter() - w_start

                # Read existing base keys
                read_bufs = [
                    bytearray(chunk_size) for _ in range(num_base_keys)
                ]

                r_start = time.perf_counter()
                client.batch_get_sync(
                    base_keys, [memoryview(b) for b in read_bufs]
                )
                r_elapsed = time.perf_counter() - r_start

                w_bytes = num_new_per_round * chunk_size
                r_bytes = num_base_keys * chunk_size
                w_tp = _format_throughput(w_bytes, w_elapsed)
                r_tp = _format_throughput(r_bytes, r_elapsed)
                print(
                    f"\n  MIXED round {r}: write={w_tp}, read={r_tp}"
                )
        finally:
            if client is not None:
                client.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)
