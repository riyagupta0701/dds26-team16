#!/usr/bin/env bash
# Test 12: Orchestrator integration tests (Python unittest suite)
# Covers saga, 2PC, concurrency, mode switching, and orchestrator resilience.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

python -m pytest "$SCRIPT_DIR/test_orchestrator.py" -v --tb=short
