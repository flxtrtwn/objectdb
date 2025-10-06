#!/usr/bin/env bash
source "${HOME}"/.bashrc

set -euo pipefail
shopt -s nullglob globstar

uv venv --allow-existing
source .venv/bin/activate
uv sync

pre-commit install >/dev/null 2>&1
git lfs install >/dev/null 2>&1
git lfs track "*.png" >/dev/null 2>&1
git lfs track "*.jpg" >/dev/null 2>&1
