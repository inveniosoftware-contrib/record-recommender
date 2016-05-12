#!/bin/bash

pydocstyle record_recommender
isort -rc -c -df **/*.py
check-manifest --ignore ".travis-*"
py.test

# sphinx-build -qnNW docs docs/_build/html && \
# sphinx-build -qnNW -b doctest docs docs/_build/doctest
