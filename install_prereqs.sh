#!/usr/bin/env bash
set -ex
pip install -e .
pip install -r dev-requirements.txt
pre-commit install --hook-type pre-commit
