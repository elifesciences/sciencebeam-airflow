#!/bin/sh

set -e

echo "running flake8"
python -m flake8 dags tests

echo "running pylint"
python -m pylint dags tests

echo "running pytest"
python -m pytest

echo "done"
