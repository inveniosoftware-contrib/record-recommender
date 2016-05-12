# -*- coding: utf-8 -*-
#
# This file is part of CERN Document Server.
# Copyright (C) 2016 CERN.
#
# CERN Document Server is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# CERN Document Server is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with CERN Document Server; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""Record Recommender application."""

from __future__ import absolute_import, print_function

import logging
import logging.config
import os
import signal
import time
from multiprocessing import Manager, Pool

import yaml

from .fetcher import ElasticsearchFetcher
from .recommender import GraphRecommender
from .storage import FileStore

_reco = None
_store = None
logger = logging.getLogger(__name__)


def get_config(config_path='/etc/record_recommender.yml', sentry=False):
    """Load the configuration file."""
    config = {}
    if os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f.read())
    else:
        # TODO: Auto create config file.
        print("No config file at {}".format(config_path))

    es_password = os.environ.get('RECOMMENDER_ES_PASSWORD')
    if es_password:
        config.get('elasticsearch', {})['es_password'] = es_password

    sentry_os = os.environ.get('RECOMMENDER_SENTRY')
    if sentry_os:
        config['logging']['handlers']['sentry']['dsn'] = sentry_os
    elif config.get('sentry'):
        config['logging']['handlers']['sentry']['dsn'] = config.get('sentry')
    else:
        if config.get('logging.handlers.sentry'):
            config['logging']['handlers'].pop('sentry')

    return config


def setup_logging(config=None):
    """Setup logging configuration."""
    # TODO: integrate in general config file
    print(__name__)
    if config and config.get('logging'):
        logging.config.dictConfig(config.get('logging'))
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                            level=logging.DEBUG)


class RecordRecommender(object):
    """Record Recommender."""

    def __init__(self, config=None):
        """Initialize the storage and creates saves the config."""
        self.config = {'recommendation_version': 0}
        if config:
            self.config.update(config)
        self.store = FileStore(self.config)
        self.logger = logging.getLogger('app.RecordRecommender')

    def fetch_weeks(self, weeks, overwrite=False):
        """Fetch and cache the requested weeks."""
        esf = ElasticsearchFetcher(self.store, self.config)
        for year, week in weeks:
            print("Fetch {}-{}".format(year, week))
            esf.fetch(year, week, overwrite)

    def create_all_recommendations(self, cores, ip_views=False):
        """Calculate the recommendations for all records."""
        global _store
        _store = self.store
        _create_all_recommendations(cores, ip_views, self.config)


def _create_all_recommendations(cores, ip_views=False, config=None):
    """Calculate all recommendations in multiple processes."""
    global _reco, _store

    _reco = GraphRecommender(_store)
    _reco.load_profile('Profiles')
    if ip_views:
        _reco.load_profile('Profiles_IP')

    manager = Manager()
    record_list = manager.list(_reco.all_records.keys())
    # record_list = manager.list(list(_reco.all_records.keys())[:10])
    num_records = len(_reco.all_records.keys())
    logger.info("Recommendations to build: {}".format(num_records))

    start = time.time()
    reco_version = config.get('recommendation_version', 0)
    done_records = num_records
    if cores <= 1:
        _create_recommendations(0, record_list, reco_version)
    else:
        try:
            pool = Pool(cores)
            multiple_results = [pool.apply_async(_create_recommendations,
                                (i, record_list, reco_version))
                                for i in range(cores)]
            # Wait for all processes to exit
            [res.get() for res in multiple_results]
        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, terminating workers")
            # TODO: Update done_records.
            pool.terminate()
            pool.join()

    duration = time.time() - start
    logger.info("Time {} for {} recommendations".format(duration,
                                                        done_records))


def _create_recommendations(worker, record_list, reco_version):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    print("Worker {} started".format(worker))
    redis = _store.get_recommendation_store()
    number_records = 0
    # TODO: Return calculation time.
    while True:
        try:
            recid = record_list.pop()
        except IndexError:
            print("End {}".format(worker))
            return number_records
        print("Worker {} building record: {}".format(worker, recid))
        try:
            nodes, weights = _reco.recommend_for_record(recid)
            recommendations = {'records': nodes,
                               'version': reco_version}
            redis.set(recid, recommendations)
        except:
            logger.exception("Exception in Worker when calculating %s", recid,
                             exc_info=True)
        number_records += 1
