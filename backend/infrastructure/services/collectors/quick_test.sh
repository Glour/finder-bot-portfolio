#!/bin/bash
# Quick test script for orderbook pipeline optimizations
# Usage: ./quick_test.sh [baseline|cache|streams|full]

set -e

MODE="${1:-baseline}"
DURATION="${2:-60}"

echo "=========================================="
echo "Orderbook Pipeline Quick Test"
echo "=========================================="
echo "Mode: $MODE"
echo "Duration: ${DURATION}s"
echo "=========================================="

cd "$(dirname "$0")/spread_collector"

case "$MODE" in
    baseline)
        echo "Testing BASELINE (original implementation)"
        python main.py
        ;;

    cache)
        echo "Testing PHASE 1 (token metadata cache)"
        export OPTIMIZE_TOKEN_CACHE=true
        export OPTIMIZE_ENABLE_PROFILING=true
        python main_optimized.py
        ;;

    streams)
        echo "Testing PHASE 2 (Redis Streams)"
        export OPTIMIZE_TOKEN_CACHE=true
        export OPTIMIZE_USE_STREAMS=true
        export OPTIMIZE_ENABLE_PROFILING=true
        export OPTIMIZE_ENABLE_LATENCY_TRACKING=true
        python main_optimized.py
        ;;

    full)
        echo "Testing FULL OPTIMIZATION (all phases)"
        export OPTIMIZE_TOKEN_CACHE=true
        export OPTIMIZE_USE_STREAMS=true
        export OPTIMIZE_MSGPACK_ONLY=true
        export OPTIMIZE_ENABLE_PROFILING=true
        export OPTIMIZE_ENABLE_LATENCY_TRACKING=true
        python main_optimized.py
        ;;

    *)
        echo "Unknown mode: $MODE"
        echo "Available modes: baseline, cache, streams, full"
        exit 1
        ;;
esac
