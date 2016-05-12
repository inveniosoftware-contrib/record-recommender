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

"""Manages the user profiles."""

from __future__ import absolute_import, print_function

import hashlib
import logging
from collections import defaultdict

from six import iteritems

logger = logging.getLogger(__name__)


class Profiles(object):
    """Create user profiles from pageviews and downloads."""

    def __init__(self, storage, config=None):
        """Constructor."""
        self.storage = storage
        self.stat = {'records_all': 0,
                     'user_record_events': 0,
                     }
        self.config = {'user_views_min': 2,
                       'user_views_max': 400,
                       }
        if config:
            self.config.update(config)
        self.stat_long = defaultdict(list)

    def create(self, weeks):
        """Create the user and ip profiles for the given weeks."""
        user_pageviews = self.create_profiles('Pageviews', weeks)
        user_downloads = self.create_profiles('Downloads', weeks)

        self._export_profiles('Profiles', user_pageviews, user_downloads)

        user_pageviews = self.create_profiles('Pageviews_IP', weeks, True)
        user_downloads = self.create_profiles('Downloads_IP', weeks, True)

        self._export_profiles('Profiles_IP', user_pageviews, user_downloads,
                              ip_user=True)

    def _export_profiles(self, profile_name, user_pageviews, user_downloads,
                         ip_user=False):
        """Filter and export the user profiles."""
        views_min = self.config.get('user_views_min')
        views_max = self.config.get('user_views_max')
        ip_user_id = 500000000000
        add_user_id = 100000000000
        stat_records = 0
        with self.storage.get_user_profiles(profile_name) as store:
            store.clear()
            for user in user_pageviews:
                # Only users with unique pageviews.
                unique_views = len(set(user_pageviews[user]))
                if views_max > unique_views >= views_min:
                    nodes, weight = self._calculate_user_record_weights(
                            record_list=user_pageviews[user],
                            download_list=user_downloads.get(user))
                    if ip_user:
                        store.add_user(ip_user_id, nodes, weight)
                        ip_user_id += 1
                    else:
                        user = str(add_user_id + int(user))
                        store.add_user(user, nodes, weight)
                    self.stat_long['User_num_records'].append(len(nodes))
                    stat_records += len(nodes)

                elif unique_views >= views_min:
                    # TODO: Add stat for to many views.
                    print("Drop user {} with {} views".format(user,
                                                              unique_views))
        self.stat['user_profiles'] = len(self.stat_long.get(
                                                        'User_num_records'))
        self.stat['user_profiles_records'] = stat_records

        print("Stats: {}".format(self.stat))

    def create_profiles(self, prefix, weeks, ip_user=False):
        """Create the user profiles for the given weeks."""
        # Future: Add a time range in weeks for how long a user is considered
        #         as the same user.

        # Count accessed records
        record_counter = {}
        for year, week in weeks:
            file = self.storage.get(prefix, year, week)
            self.count_records(record_counter, file)

        # TODO: Statistics, count records
        print("Records read all: {}".format(self.stat))

        # Filter records with to less/much views.
        records_valid = self.filter_counter(record_counter)

        # Create user profiles
        profiles = defaultdict(list)
        for year, week in weeks:
            file = self.storage.get(prefix, year, week)
            self._create_user_profiles(profiles, file, records_valid, ip_user,
                                       year, week)

        return profiles

    def count_records(self, record_counter, file):
        """Count the number of viewed records."""
        counter = record_counter
        events_counter = 0
        for record in file.get_records():
            recid = record[2]
            counter[recid] = counter.get(recid, 0) + 1
            events_counter += 1

        self.stat['user_record_events'] = events_counter
        return counter

    def filter_counter(self, counter, min=2, max=100000000):
        """
        Filter the counted records.

        Returns: List with record numbers.
        """
        records_filterd = {}
        counter_all_records = 0
        for item in counter:
            counter_all_records += 1
            if max > counter[item] >= min:
                records_filterd[item] = counter[item]

        self.stat['user_record_events'] = counter_all_records
        self.stat['records_filtered'] = len(records_filterd)
        return records_filterd

    def _create_user_profiles(self, profiles, file, valid_records,
                              ip_user=False, year=None, week=None):
        """
        Create user profiles with all the records visited or downloaded.

        Returns: Dictionary with the user id and a record list.
        {'2323': [1, 2, 4]}
        """
        for record in file.get_records():
            recid = record[2]
            if not valid_records.get(recid, None):
                # Record not valid
                continue

            if ip_user:
                ip = record[4]
                user_agent = record[5]
                # Generate unique user id
                user_id = "{0}-{1}_{2}_{3}".format(year, week, ip, user_agent)
                try:
                    uid = hashlib.md5(user_id.encode('utf-8')).hexdigest()
                except UnicodeDecodeError:
                    logger.info("UnicodeDecodeError {}".format(user_id))
            else:
                uid = record[1]

            profiles[uid].append(recid)

        return profiles

    def _calculate_user_record_weights(self, record_list,
                                       basic_weight=0.3,
                                       max_views=20,
                                       download_list=None,
                                       basic_weight_download=0.5):
        new_nodes = []
        new_nodes_weight = []
        node_dict = {}
        # Count how often a record is viewed
        try:
            for node_id in record_list:
                node_dict[int(node_id)] = node_dict.get(int(node_id), 0) + 1
        except ValueError as e:
            logger.exception('ValuerError')
            return new_nodes, new_nodes_weight

        for node, count in iteritems(node_dict):
            # Initial weight for each node
            weight = basic_weight
            if download_list and (str(node) in download_list):
                weight = basic_weight_download
            if count > 1:
                # only count to max_views/visits
                if count > max_views:
                    count = max_views
                # max weight given for 7 views is 0.35
                # weight_views = (float(count) / (float(count) + 13))
                weight_views = (
                    float(1 / float(75)) * float(count) + 1 / float(30))
                weight = weight + weight_views

            new_nodes.append(node)
            new_nodes_weight.append(weight)

        return new_nodes, new_nodes_weight
