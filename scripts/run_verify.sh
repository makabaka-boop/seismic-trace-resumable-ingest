#!/bin/sh
# Convenience entrypoint for the one-shot acceptance service.
# Usage (from repo root):
#   docker compose --profile verify up --build verify
set -e
exec pytest -v --tb=short "$@"
