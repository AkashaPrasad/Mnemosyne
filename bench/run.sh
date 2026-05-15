#!/bin/bash
# Usage: ./bench/run.sh
# Ingests the published sample, runs the canonical scenario, emits JSON report

set -euo pipefail

REPO_DIR="${BENCH_REPO:-../Anvil-P-E}"
BENCH_DIR="$REPO_DIR/bench-p02-context"

if [ ! -d "$BENCH_DIR" ]; then
    echo "ERROR: Benchmark repo not found at $REPO_DIR"
    echo "Run: git clone https://github.com/Sauhard74/Anvil-P-E ../Anvil-P-E"
    exit 1
fi

cd "$BENCH_DIR"

python run.py \
    --adapter adapters.mnemosyne:Engine \
    --mode fast \
    --seeds 42 \
    --out report.json

echo "Report written to $BENCH_DIR/report.json"
