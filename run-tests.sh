#!/bin/bash

# -x Shows the executed commands and -e exit immediately if a command fails.
set -x -e
pydocstyle record_recommender
isort -rc -c -df **/*.py
check-manifest --ignore ".travis-*"
python setup.py test

# sphinx-build -qnNW docs docs/_build/html && \
# sphinx-build -qnNW -b doctest docs docs/_build/doctest
