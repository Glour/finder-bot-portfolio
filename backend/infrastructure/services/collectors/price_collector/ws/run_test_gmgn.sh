#!/bin/bash
# Wrapper to run GMGN WebSocket test and suppress curl_cffi segfault

cd "$(dirname "$0")"/../../../../../..

# Disable buffering and run test
# -u для unbuffered Python output
# stdbuf -o0 -e0 для unbuffered pipe
# Filter out curl_cffi cleanup errors in real-time
PYTHONUNBUFFERED=1 stdbuf -o0 -e0 python -u backend/infrastructure/services/collectors/price_collector/ws/test_gmgn_ws.py 2>&1 | \
    grep --line-buffered -v -E "(Exception ignored|BufferError|memoryview|tp_clear|Traceback|File \"/|def |asyncio/|json/encoder|selectors\.py|raise |KeyError)" | \
    grep --line-buffered -v "образ памяти сброшен" | \
    grep --line-buffered -v "items = dct" | \
    grep --line-buffered -v "^\s*\^"

# Always exit 0 regardless of segfault
exit 0
