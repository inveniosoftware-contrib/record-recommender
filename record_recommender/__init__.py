
"""Record Recommender for Invenio."""

from __future__ import absolute_import, print_function

from .cli import cli
from .app import RecordRecommender, get_config
from .utils import get_last_weeks

all = [cli, RecordRecommender, get_config, get_last_weeks]
