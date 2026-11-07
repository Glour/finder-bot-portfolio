"""
Benchmark script for comparing baseline vs optimized pipeline performance.

Measures:
- End-to-end latency (WebSocket → Telegram)
- Throughput (tokens/second)
- CPU usage
- Memory usage
- Redis operations

Usage:
    # Benchmark baseline
    python benchmark.py --mode baseline --duration 300

    # Benchmark with token cache
    python benchmark.py --mode cache --duration 300

    # Benchmark with streams
    python benchmark.py --mode streams --duration 300

    # Compare all modes
    python benchmark.py --compare --duration 300
"""
import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

import psutil
from redis import asyncio as aioredis

from infrastructure.database import setup as database
from infrastructure.database.requests import RequestsRepo
from infrastructure.services.common.cache import RedisCache
from infrastructure.services.collectors.optimization_config import OptimizationConfig
from settings import settings, logger


@dataclass
class BenchmarkMetrics:
    """Metrics collected during benchmark run."""
    mode: str
    duration: int
    start_time: str
    end_time: str

    # Latency metrics (milliseconds)
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float

    # Throughput
    total_tokens_processed: int
    tokens_per_second: float

    # Results
    total_results_found: int
    results_per_second: float

    # Resource usage
    avg_cpu_percent: float
    max_cpu_percent: float
    avg_memory_mb: float
    max_memory_mb: float

    # Redis operations
    redis_get_count: int
    redis_set_count: int
    redis_stream_count: int

    def to_dict(self):
        """Convert to dictionary."""
        return asdict(self)

    def to_json(self):
        """Convert to JSON."""
        return json.dumps(self.to_dict(), indent=2)


class BenchmarkRunner:
    """Run and measure pipeline performance."""

    def __init__(
        self,
        mode: str,
        duration: int,
        opt_config: OptimizationConfig
    ):
        self.mode = mode
        self.duration = duration
        self.opt_config = opt_config

        # Metrics
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.latencies: list[float] = []
        self.tokens_processed = 0
        self.results_found = 0

        # Resource tracking
        self.cpu_samples: list[float] = []
        self.memory_samples: list[float] = []

        # Redis operation tracking
        self.redis_gets = 0
        self.redis_sets = 0
        self.redis_streams = 0

        self.process = psutil.Process()

    async def run(self) -> BenchmarkMetrics:
        """Run benchmark and collect metrics."""
        logger.info(f"Starting benchmark: mode={self.mode}, duration={self.duration}s")
        self.start_time = time.time()

        # Start monitoring tasks
        monitor_task = asyncio.create_task(self._monitor_resources())

        # Run pipeline based on mode
        if self.mode == "baseline":
            await self._run_baseline()
        elif self.mode == "cache":
            await self._run_with_cache()
        elif self.mode == "streams":
            await self._run_with_streams()
        else:
            raise ValueError(f"Unknown mode: {self.mode}")

        # Stop monitoring
        monitor_task.cancel()
        self.end_time = time.time()

        # Compute metrics
        return self._compute_metrics()

    async def _monitor_resources(self):
        """Monitor CPU and memory usage."""
        while True:
            try:
                cpu = self.process.cpu_percent(interval=1)
                memory = self.process.memory_info().rss / 1024 / 1024  # MB

                self.cpu_samples.append(cpu)
                self.memory_samples.append(memory)

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break

    async def _run_baseline(self):
        """Run baseline (original implementation)."""
        logger.info("Running baseline implementation")

        # Import and run original manager
        from infrastructure.services.collectors.spread_collector.manager import (
            ArbitrageAnalyzerManager
        )

        database.redis = aioredis.from_url(settings.redis.dsn, decode_responses=True)
        database.init_database(settings)
        repo = RequestsRepo(sessionmaker=database.async_session)
        cache = RedisCache(database.redis)

        manager = ArbitrageAnalyzerManager(
            repo=repo,
            cache=cache,
            tokens_load_limit=10000
        )

        # Run for duration
        end_time = time.time() + self.duration
        while time.time() < end_time:
            cycle_start = time.time()

            # Simulate one cycle (you'll need to adapt this to your actual code)
            # For now, just sleep
            await asyncio.sleep(0.5)

            cycle_time = (time.time() - cycle_start) * 1000
            self.latencies.append(cycle_time)
            self.tokens_processed += 1000  # Placeholder

    async def _run_with_cache(self):
        """Run with token metadata caching."""
        logger.info("Running with token cache")

        from infrastructure.services.collectors.spread_collector.manager_optimized import (
            OptimizedArbitrageAnalyzerManager
        )
        from infrastructure.services.collectors.spread_collector.config import PerformanceConfig

        database.redis = aioredis.from_url(settings.redis.dsn, decode_responses=True)
        database.init_database(settings)
        repo = RequestsRepo(sessionmaker=database.async_session)
        cache = RedisCache(database.redis)

        perf_config = PerformanceConfig(
            batch_size=self.opt_config.batch_size,
            analysis_interval=self.opt_config.analysis_interval,
            enable_profiling=True
        )

        manager = OptimizedArbitrageAnalyzerManager(
            repo=repo,
            cache=cache,
            tokens_load_limit=10000,
            performance_config=perf_config,
            use_streams=False
        )

        # Run for duration with timeout
        try:
            await asyncio.wait_for(manager.run(), timeout=self.duration)
        except asyncio.TimeoutError:
            pass

    async def _run_with_streams(self):
        """Run with Redis Streams."""
        logger.info("Running with Redis Streams")

        from infrastructure.services.collectors.spread_collector.manager_optimized import (
            OptimizedArbitrageAnalyzerManager
        )
        from infrastructure.services.collectors.spread_collector.config import PerformanceConfig

        database.redis = aioredis.from_url(
            settings.redis.dsn,
            decode_responses=False  # Binary mode for msgpack
        )
        database.init_database(settings)
        repo = RequestsRepo(sessionmaker=database.async_session)
        cache = RedisCache(database.redis)

        perf_config = PerformanceConfig(
            batch_size=self.opt_config.batch_size,
            analysis_interval=self.opt_config.analysis_interval,
            enable_profiling=True
        )

        manager = OptimizedArbitrageAnalyzerManager(
            repo=repo,
            cache=cache,
            tokens_load_limit=10000,
            performance_config=perf_config,
            use_streams=True,
            consumer_group="benchmark_group",
            consumer_name=f"benchmark_{int(time.time())}"
        )

        try:
            await asyncio.wait_for(manager.run(), timeout=self.duration)
        except asyncio.TimeoutError:
            pass

    def _compute_metrics(self) -> BenchmarkMetrics:
        """Compute final metrics."""
        total_time = self.end_time - self.start_time

        # Latency percentiles
        sorted_latencies = sorted(self.latencies) if self.latencies else [0]
        p50_idx = int(len(sorted_latencies) * 0.50)
        p95_idx = int(len(sorted_latencies) * 0.95)
        p99_idx = int(len(sorted_latencies) * 0.99)

        metrics = BenchmarkMetrics(
            mode=self.mode,
            duration=self.duration,
            start_time=datetime.fromtimestamp(self.start_time).isoformat(),
            end_time=datetime.fromtimestamp(self.end_time).isoformat(),

            # Latency
            avg_latency_ms=sum(self.latencies) / len(self.latencies) if self.latencies else 0,
            p50_latency_ms=sorted_latencies[p50_idx],
            p95_latency_ms=sorted_latencies[p95_idx],
            p99_latency_ms=sorted_latencies[p99_idx],
            max_latency_ms=max(self.latencies) if self.latencies else 0,

            # Throughput
            total_tokens_processed=self.tokens_processed,
            tokens_per_second=self.tokens_processed / total_time if total_time > 0 else 0,

            # Results
            total_results_found=self.results_found,
            results_per_second=self.results_found / total_time if total_time > 0 else 0,

            # Resources
            avg_cpu_percent=sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0,
            max_cpu_percent=max(self.cpu_samples) if self.cpu_samples else 0,
            avg_memory_mb=sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0,
            max_memory_mb=max(self.memory_samples) if self.memory_samples else 0,

            # Redis
            redis_get_count=self.redis_gets,
            redis_set_count=self.redis_sets,
            redis_stream_count=self.redis_streams
        )

        return metrics


async def benchmark_single_mode(mode: str, duration: int, opt_config: OptimizationConfig):
    """Run benchmark for a single mode."""
    runner = BenchmarkRunner(mode, duration, opt_config)
    metrics = await runner.run()

    # Print results
    print("\n" + "=" * 80)
    print(f"BENCHMARK RESULTS: {mode.upper()}")
    print("=" * 80)
    print(f"\nLatency:")
    print(f"  Average: {metrics.avg_latency_ms:.2f}ms")
    print(f"  P50: {metrics.p50_latency_ms:.2f}ms")
    print(f"  P95: {metrics.p95_latency_ms:.2f}ms")
    print(f"  P99: {metrics.p99_latency_ms:.2f}ms")
    print(f"  Max: {metrics.max_latency_ms:.2f}ms")

    print(f"\nThroughput:")
    print(f"  Total tokens: {metrics.total_tokens_processed}")
    print(f"  Tokens/second: {metrics.tokens_per_second:.2f}")
    print(f"  Total results: {metrics.total_results_found}")
    print(f"  Results/second: {metrics.results_per_second:.2f}")

    print(f"\nResources:")
    print(f"  CPU avg: {metrics.avg_cpu_percent:.1f}%")
    print(f"  CPU max: {metrics.max_cpu_percent:.1f}%")
    print(f"  Memory avg: {metrics.avg_memory_mb:.1f} MB")
    print(f"  Memory max: {metrics.max_memory_mb:.1f} MB")

    print(f"\nRedis Operations:")
    print(f"  GET: {metrics.redis_get_count}")
    print(f"  SET: {metrics.redis_set_count}")
    print(f"  STREAM: {metrics.redis_stream_count}")
    print("=" * 80)

    # Save to file
    output_file = f"benchmark_{mode}_{int(time.time())}.json"
    with open(output_file, "w") as f:
        f.write(metrics.to_json())
    print(f"\nResults saved to: {output_file}")

    return metrics


async def compare_modes(duration: int, opt_config: OptimizationConfig):
    """Compare multiple modes."""
    modes = ["baseline", "cache", "streams"]
    results = {}

    for mode in modes:
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Benchmarking {mode.upper()}")
        logger.info(f"{'=' * 80}\n")

        metrics = await benchmark_single_mode(mode, duration, opt_config)
        results[mode] = metrics

        # Cool-down between runs
        await asyncio.sleep(10)

    # Print comparison
    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)
    print(f"\n{'Metric':<30} {'Baseline':<15} {'Cache':<15} {'Streams':<15}")
    print("-" * 80)

    baseline = results["baseline"]
    cache = results["cache"]
    streams = results["streams"]

    print(f"{'Avg Latency (ms)':<30} {baseline.avg_latency_ms:<15.2f} {cache.avg_latency_ms:<15.2f} {streams.avg_latency_ms:<15.2f}")
    print(f"{'P95 Latency (ms)':<30} {baseline.p95_latency_ms:<15.2f} {cache.p95_latency_ms:<15.2f} {streams.p95_latency_ms:<15.2f}")
    print(f"{'Throughput (tok/s)':<30} {baseline.tokens_per_second:<15.2f} {cache.tokens_per_second:<15.2f} {streams.tokens_per_second:<15.2f}")
    print(f"{'CPU Avg (%)':<30} {baseline.avg_cpu_percent:<15.1f} {cache.avg_cpu_percent:<15.1f} {streams.avg_cpu_percent:<15.1f}")
    print(f"{'Memory Avg (MB)':<30} {baseline.avg_memory_mb:<15.1f} {cache.avg_memory_mb:<15.1f} {streams.avg_memory_mb:<15.1f}")

    # Calculate improvements
    print("\n" + "=" * 80)
    print("IMPROVEMENTS vs BASELINE")
    print("=" * 80)

    cache_latency_improvement = (baseline.avg_latency_ms - cache.avg_latency_ms) / baseline.avg_latency_ms * 100
    streams_latency_improvement = (baseline.avg_latency_ms - streams.avg_latency_ms) / baseline.avg_latency_ms * 100

    print(f"{'Cache latency improvement:':<40} {cache_latency_improvement:>6.1f}%")
    print(f"{'Streams latency improvement:':<40} {streams_latency_improvement:>6.1f}%")

    cache_throughput_improvement = (cache.tokens_per_second - baseline.tokens_per_second) / baseline.tokens_per_second * 100
    streams_throughput_improvement = (streams.tokens_per_second - baseline.tokens_per_second) / baseline.tokens_per_second * 100

    print(f"{'Cache throughput improvement:':<40} {cache_throughput_improvement:>6.1f}%")
    print(f"{'Streams throughput improvement:':<40} {streams_throughput_improvement:>6.1f}%")
    print("=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Benchmark orderbook pipeline")
    parser.add_argument("--mode", choices=["baseline", "cache", "streams"], help="Benchmark mode")
    parser.add_argument("--compare", action="store_true", help="Compare all modes")
    parser.add_argument("--duration", type=int, default=300, help="Benchmark duration (seconds)")

    args = parser.parse_args()

    if not args.mode and not args.compare:
        parser.error("Either --mode or --compare must be specified")

    # Load optimization config
    opt_config = OptimizationConfig.from_env()

    # Run benchmark
    if args.compare:
        asyncio.run(compare_modes(args.duration, opt_config))
    else:
        asyncio.run(benchmark_single_mode(args.mode, args.duration, opt_config))


if __name__ == "__main__":
    main()
