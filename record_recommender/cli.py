
"""Record Recommender command line interface."""

from __future__ import absolute_import, print_function

import click
from IPython import embed

from .app import RecordRecommender, get_config, setup_logging
from .profiles import Profiles
from .recommender import GraphRecommender
from .storage import FileStore
from .utils import get_last_weeks

config = None
store = None


@click.group()
# @click.option('--config', nargs=2, multiple=True,
#               metavar='KEY VALUE', help='Overrides a config key/value pair.')
@click.option('--verbose', '-v', is_flag=True,
              help='Enables verbose mode.')
@click.option('--config_path', '-c',
              help='Path to the configuration file.')
def cli(config_path, verbose):
    """Record-Recommender command line version."""
    global config, store
    if not config_path:
        config_path = '/etc/record_recommender.yml'
    config = get_config(config_path)
    setup_logging(config)
    store = FileStore(config)


@cli.command()
def debug():
    """Debug the application and recommender."""
    reco = GraphRecommender(store)
    print('# Load the user profiles into the graph.')
    print("graph = reco.load_profile('Profiles')")
    print('# Load the ip profiles into the graph.')
    print("graph = reco.load_profile('Profiles_IP')")

    embed()


@cli.command()
@click.argument('weeks', type=int)
@click.option('--force', '-f', is_flag=True,
              help='Force file overwriting.')
def fetch(weeks, force):
    """Fetch newest PageViews and Downloads."""
    weeks = get_last_weeks(weeks)
    print(weeks)
    recommender = RecordRecommender(config)
    recommender.fetch_weeks(weeks, overwrite=force)


@cli.command()
@click.argument('weeks', type=int)
@click.argument('cores', type=int)
@click.option('--ip-views', '-ip', is_flag=True,
              help='Also uses IP based user profiles.')
def update_recommender(weeks, cores, ip_views):
    """
    Download and build the recommendations.

    - Fetch new statistics from the current week.
    - Generate recommendations.
    - Update the recommendations.
    """
    weeks = get_last_weeks(weeks)
    recommender = RecordRecommender(config)
    # Redownload incomplete weeks
    first_weeks = weeks[:2]
    recommender.fetch_weeks(first_weeks, overwrite=True)
    # Download missing weeks
    recommender.fetch_weeks(weeks, overwrite=False)

    print("Build Profiles")
    profiles(weeks)

    print("Generate Recommendations")
    build(cores, ip_views)


@cli.command()
@click.argument('weeks', type=int)
def profiles(weeks):
    """
    Number of weeks to build.

    Starting with the current week.
    """
    profiles = Profiles(store)
    weeks = get_last_weeks(weeks)
    print(weeks)
    profiles.create(weeks)


@cli.command()
@click.argument('processes', type=int)
# @click.option('--no-ip-views', '-ip', is_flag=False,
#               help='Also uses IP based user profiles.')
def build(processes):
    """
    Calculate all recommendations using the number of specified processes.

    The recommendations are calculated from the generated Profiles file.
    """
    recommender = RecordRecommender(config)
    recommender.create_all_recommendations(processes, ip_views=True)
