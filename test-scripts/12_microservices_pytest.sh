#!/usr/bin/env bash
# Test 12: Microservices integration tests (Python unittest suite)
# Covers stock, payment, and order service correctness.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

python -m pytest "$SCRIPT_DIR/test_microservices.py" -v --tb=short
