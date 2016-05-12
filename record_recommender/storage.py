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

"""Manages the acces to the cache files and the Redis connection."""

from __future__ import absolute_import, print_function

import csv
import json
import os

import numpy as np
from redis import Redis

from .utils import get_year_week


class File(object):
    """A File holding pageviews or downloads."""

    def __init__(self, path, prefix, fields):
        """Initialize file handler."""
        self.prefix = prefix
        self.path = path
        self.fields = fields
        self.file = None
        self._csv = None

    def __enter__(self):
        """Context manager enter."""
        return self

    def __exit__(self, type, value, traceback):
        """Context manager exit."""
        self.close()

    def clear(self):
        """Clear all content."""
        self.close()
        self.open('overwrite')

    def delete(self):
        """Delete the file."""
        self.close()
        if self.does_file_exist():
            os.remove(self.path)

    def does_file_exist(self):
        """Check if file exist."""
        return os.path.isfile(self.path)

    def open(self, mode='read'):
        """Open the file."""
        if self.file:
            self.close()
            raise 'Close file before opening.'

        if mode == 'write':
            self.file = open(self.path, 'w')
        elif mode == 'overwrite':
            # Delete file if exist.
            self.file = open(self.path, 'w+')
        else:
            # Open for reading.
            self.file = open(self.path, 'r')

        self._csv = csv.DictWriter(self.file,
                                   fieldnames=self.fields,
                                   delimiter=',', quotechar='|',
                                   quoting=csv.QUOTE_MINIMAL,
                                   extrasaction='ignore')
        if self.file.tell() == 0:
            self._csv.writeheader()

    def close(self):
        """Close the file."""
        self._csv = None
        if self.file:
            self.file.close()


class RawEvents(File):
    """A File holding pageviews or downloads."""

    def __init__(self, path, prefix, year, week):
        """Constructor."""
        self.year = year
        self.week = week
        fields = ['timestamp', 'user', 'recid', 'file_format', 'ip',
                  'user_agent']
        self.latest_timestamp = 0
        self.number_of_hits = 0
        super(RawEvents, self).__init__(path, prefix, fields)

    def add_hit(self, hit):
        """Add a hit to the file."""
        if not self._csv:
            raise 'Open before write'
        self._csv.writerow(hit)
        self.number_of_hits += 1
        # Todo: check performance for timestamp check
        # assert self._path == self.get_filename_by_timestamp(timestamp)
        timestamp = hit['timestamp']
        if self.latest_timestamp <= timestamp:
            self.latest_timestamp = timestamp

    def get_records(self):
        """
        Get all stored records.

        Returns: (timestamp, user, recid,...)
        """
        self.close()
        with open(self.path, 'r') as filep:
            first_line = filep.readline().split(',')
            if first_line[0] != self.fields[0]:
                yield first_line

            for line in filep:
                yield line.split(',')


class UserProfiles(File):
    """A File holding the raw recommendations."""

    def __init__(self, path, prefix):
        """Constructor."""
        fields = ['user', 'recid', 'score']
        super(UserProfiles, self).__init__(path, prefix, fields)

    def add_user(self, uid, nodes, weights):
        """Add a user."""
        for i, node in enumerate(nodes):
            self.file.write("{},{},{}\n".format(uid, node, weights[i]))

    def get_user_views(self):
        """
        Get all user views.

        Returns : Generator with data as ('user', 'recid', 'score')
                    for example (320, 5, 0.2)
        """
        self.close()
        with open(self.path, 'r') as filep:
            first_line = filep.readline().split(',')
            if first_line[0] != self.fields[0]:
                raise "ERROR wrong file format."

            for line in filep:
                yield line.split(',')


class RedisStore(object):
    """Redis Storage."""

    def __init__(self, host, port, db, prefix):
        """Constructor."""
        self.prefix = prefix
        self.redis = Redis(host=host, port=port, db=db)

    def get(self, key, default=None):
        """Get a key."""
        key = "{0}{1}".format(self.prefix, key)

        data = self.redis.get(key)
        # Redis returns None not an exception
        if data is None:
            data = default
        else:
            data = json.loads(data)

        return data

    def set(self, key, value):
        """Set a key, value pair."""
        key = "{0}{1}".format(self.prefix, key)

        value = json.dumps(value, cls=NumpyEncoder)
        self.redis.set(key, value)


class NumpyEncoder(json.JSONEncoder):
    """Encode Numpy objects."""

    def default(self, obj):
        """Default encoder."""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NumpyEncoder, self).default(obj)


class FileStore(object):
    """File storage to manage all cached files."""

    pageviews = 'Pageviews'
    downloads = 'Downloads'
    pageviews_ip = 'Pageviews_IP'
    downloads_ip = 'Downloads_IP'

    def __init__(self, config=None):
        """Constructor."""
        self.config = {
                       'base_path': 'cache/',
                       'cache_file_prefix': '',
                       'host': 'localhost',
                       'port': '6379',
                       'db': '0',
                       'prefix': 'Reco_1::',
                      }
        if config:
            self.config.update(config.get('cache'))
            self.config.update(config.get('redis'))
        self.base_path = self.config['base_path']
        self.prefix = self.config['cache_file_prefix']

    def get_by_timestamp(self, prefix, timestamp):
        """Get the cache file to a given timestamp."""
        year, week = get_year_week(timestamp)
        return self.get(prefix, year, week)

    def get(self, prefix, year, week):
        """Get the cache file."""
        filename = self._format_filename(prefix, year, week)
        return RawEvents(filename, prefix, year, week)

    def get_user_profiles(self, prefix):
        """Get the user profil from the cache to the given prefix."""
        filepath = "{}{}".format(self.base_path, prefix)
        return UserProfiles(filepath, prefix)

    def _format_filename(self, prefix, year, week):
        """Construct the file name based on the path and options."""
        return "{}{}_{}-{}.csv".format(self.base_path, prefix, year, week)

    def get_recommendation_store(self):
        """Get the configured recommendation store."""
        return RedisStore(self.config['host'],
                          self.config['port'],
                          self.config['db'],
                          self.config['prefix'])
