#!/bin/bash
set -euo pipefail

REPO_DIR="${BENCH_REPO:-../Anvil-P-E}"
BENCH_DIR="$REPO_DIR/bench-p02-context"

if [ ! -d "$BENCH_DIR" ]; then
    echo "ERROR: Benchmark repo not found at $REPO_DIR"
    exit 1
fi

cd "$BENCH_DIR"

echo "=== Quick self-check ==="
python self_check.py --adapter adapters.mnemosyne:Engine --quick

echo ""
echo "=== Full benchmark (5 seeds) ==="
python run.py \
    --adapter adapters.mnemosyne:Engine \
    --mode fast \
    --seeds 9999 31415 27182 16180 11235 \
    --n-services 20 --days 14 \
    --out report.json

echo ""
echo "=== Report ==="
python -c "import json; d=json.load(open('report.json')); print(json.dumps(d, indent=2))"
