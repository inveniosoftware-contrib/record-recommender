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

"""Fetches the PageViews and Downloads."""

from __future__ import absolute_import, print_function

import logging
import re
import time

import urllib3
from elasticsearch import exceptions as esd_exceptions
from elasticsearch import Elasticsearch

from .utils import get_week_dates

urllib3.disable_warnings()


logger = logging.getLogger(__name__)


class ElasticsearchFetcher(object):
    """Fetch downloads and page views from Elasticsearch."""

    ES_QUERY = """{
    "query": {
        "filtered": {
        "query": {
            "query_string": {
                "query": "_type:%(event_name)s %(query_add)s"
            }
        },
        "filter": {
            "range": {
                "@timestamp": {
                    "gt": %(timestamp_start)s ,
                    "lt": %(timestamp_end)s
                }
            }
        }
        }
    }
    }"""

    def __init__(self, storage, config=None):
        """Constructor."""
        self.storage = storage
        self.config = {'es_index': [],
                       # 'query_size': 2000,
                       # 'query_scroll_size': '10m',
                       'overwrite_files': False,
                       'es_user': '',
                       'es_password': '',
                       'es_host': '',
                       'es_port': '443'
                       }
        if config:
            self.config.update(config.get('elasticsearch'))
        es_string = "https://{}:{}@{}:{}".format(self.config['es_user'],
                                                 self.config['es_password'],
                                                 self.config['es_host'],
                                                 self.config['es_port'],
                                                 )
        self._esd = Elasticsearch(hosts=[es_string], timeout=900)
        # Check connection to Elasticsearch.
        self._esd.ping()
        self._download_filter = []

    def fetch(self, year, week, overwrite=False):
        """Fetch PageViews and Downloads from Elasticsearch."""
        self.config['overwrite_files'] = overwrite
        time_start = time.time()
        self._fetch_pageviews(self.storage, year, week, ip_users=False)
        self._fetch_downloads(self.storage, year, week, ip_users=False)
        # CDS has no user_agent before this date 1433400000:
        self._fetch_pageviews(self.storage, year, week, ip_users=True)
        self._fetch_downloads(self.storage, year, week, ip_users=True)
        logger.info('Fetch %s-%s in %s seconds.', year, week,
                    time.time() - time_start)

    def _fetch_pageviews(self, storage, year, week, ip_users=False):
        """
        Fetch PageViews from Elasticsearch.

        :param time_from: Staring at timestamp.
        :param time_to: To timestamp
        """
        prefix = 'Pageviews'
        if ip_users:
            query_add = "AND !(bot:True) AND (id_user:0)"
            prefix += '_IP'
        else:
            query_add = "AND !(bot:True) AND !(id_user:0)"
        store = self.storage.get(prefix, year, week)
        if not self.config['overwrite_files'] and store.does_file_exist():
            logger.debug("File already exist, skip: {}-{}".format(year, week))
            return

        store.open('overwrite')

        time_from, time_to = get_week_dates(year, week, as_timestamp=True)
        es_type = "events.pageviews"
        es_query = self.ES_QUERY % {'timestamp_start': time_from * 1000,
                                    'timestamp_end': time_to * 1000,
                                    'event_name': es_type,
                                    'query_add': query_add}

        logger.info("{}: {} - {}".format(es_type, time_from, time_to))
        for hit in self._fetch_elasticsearch(es_query):
            item = {}
            try:
                item['user'] = hit['_source'].get('id_user')
                if ip_users:
                    assert 0 == item['user']
                else:
                    assert 0 != item['user']
                assert es_type == hit['_type']

                item['timestamp'] = float(hit['_source']['@timestamp']) / 1000

                if ip_users:
                    item['ip'] = str(hit['_source'].get('client_host'))
                    user_agent = str(hit['_source'].get('user_agent'))
                    if user_agent is None or user_agent == 'None':
                        continue
                    elif _is_bot(user_agent):
                        continue
                    item['user_agent'] = user_agent
                item['recid'] = int(hit['_source'].get('id_bibrec'))

            except UnicodeEncodeError as e:
                # TODO: Error logging.
                # print(e)
                continue

            # Save entry
            store.add_hit(item)
        store.close()
        # Delete File if no hits were added.
        if store.number_of_hits == 0:
            store.delete()

    def _fetch_downloads(self, storage, year, week, ip_users=False):
        """
        Fetch Downloads from Elasticsearch.

        :param time_from: Staring at timestamp.
        :param time_to: To timestamp
        """
        prefix = 'Downloads'
        if ip_users:
            query_add = "AND !(bot:True) AND (id_user:0)"
            prefix += '_IP'
        else:
            query_add = "AND !(bot:True) AND !(id_user:0)"
        store = self.storage.get(prefix, year, week)
        if not self.config['overwrite_files'] and store.does_file_exist():
            logger.debug("File already exist, skip: {}-{}".format(year, week))
            return

        store.open('overwrite')

        time_from, time_to = get_week_dates(year, week, as_timestamp=True)
        es_type = "events.downloads"
        es_query = self.ES_QUERY % {'timestamp_start': time_from * 1000,
                                    'timestamp_end': time_to * 1000,
                                    'event_name': es_type,
                                    'query_add': query_add}

        logger.info("{}: {} - {}".format(es_type, time_from, time_to))
        for hit in self._fetch_elasticsearch(es_query):
            item = {}
            try:
                item['user'] = hit['_source'].get('id_user')
                if ip_users:
                    assert 0 == item['user']
                else:
                    assert 0 != item['user']
                assert 'events.downloads' == hit['_type']

                item['timestamp'] = float(hit['_source']['@timestamp']) / 1000

                if ip_users:
                    item['ip'] = str(hit['_source'].get('client_host'))
                    item['user_agent'] = str(hit['_source'].get('user_agent'))
                    user_agent = str(hit['_source'].get('user_agent'))
                    if user_agent is None or user_agent == 'None':
                        continue
                    elif _is_bot(item['user_agent']):
                        continue
                    item['user_agent'] = user_agent

                item['recid'] = int(hit['_source'].get('id_bibrec'))

                file_format = str(hit['_source'].get('file_format'))
                if len(file_format) >= 35:
                    print("Error: file_format to long {}".format(file_format))
                    continue
                else:
                    if _is_download(file_format):
                        item['file_format'] = file_format
                    else:
                        # TODO: Find more file formats
                        continue

            except UnicodeEncodeError as e:
                # TODO: Error logging.
                # print(e)
                continue

            except KeyError as e:
                # TODO: Error logging.
                print(hit)
                print(e)
                continue
            # Save entry
            store.add_hit(item)
        store.close()
        # Delete File if no hits were added.
        if store.number_of_hits == 0:
            store.delete()

    def _fetch_elasticsearch(self, es_query):
        """
        Load data from Elasticsearch.

        :param query: TODO
        :param time_from: TODO
        :param time_to: TODO
        :returns: TODO
        """
        # TODO: Show error if index is not found.
        scanResp = self._esd.search(index=self.config['es_index'],
                                    body=es_query, size=2000,
                                    search_type="scan", scroll="10000",
                                    timeout=900, request_timeout=900)
        resp = dict(scanResp)
        resp.pop('_scroll_id')
        logger.debug(resp)
        scroll_hits = scanResp['hits']['total']
        scrollTime = scanResp['took']
        scrollId = scanResp['_scroll_id']
        # Check for shard errors
        if scanResp['_shards']['failed'] > 0:
            logger.warn("Failing shards, check ES")
        retry_count = 0
        number_of_retrys = 5
        hit_count = 0

        while True:
            try:
                response = self._esd.scroll(scroll_id=scrollId, scroll="10000",
                                            request_timeout=900)
                if response['_scroll_id'] != scrollId:
                    scrollId = response['_scroll_id']
                if scanResp['_shards']['failed'] > 0:
                    print("Failing shards, check ES")
                # No more hits
                if len(response["hits"]["hits"]) == 0:
                    break
            except esd_exceptions.ConnectionTimeout:
                logger.warning("ES exceptions: Connection Timeout")
                if retry_count >= number_of_retrys:
                    raise esd_exceptions.ConnectionTimeout()

                retry_count += 1
                continue

            except StopIteration:
                break

            except Exception as e:
                # TODO: Logging
                logging.exception("ES exception", exc_info=True)
                print("EXCEPTION")
                print(e)
                break

            for hit in response["hits"]["hits"]:
                yield hit
                hit_count += 1

        if hit_count > scroll_hits:
            # More hits as expected, happens sometimes.
            logger.info('More hits as expected %s/%s', hit_count, scroll_hits)
        elif hit_count < scroll_hits:
            # Less hits as expected, something went wrong.
            logger.warn('Less hits as expected %s/%s', hit_count, scroll_hits)
        logger.info('%s Hits', hit_count)


def _is_bot(user_agent):
    """Check if user_agent is a known bot."""
    bot_list = [
                'http://www.baidu.com/search/spider.html',
                'python-requests',
                'http://ltx71.com/',
                'http://drupal.org/',
                'www.sogou.com',
                'http://search.msn.com/msnbot.htm',
                'semantic-visions.com crawler',
               ]
    for bot in bot_list:
        if re.search(re.escape(bot), user_agent):
            return True
    return False


def _is_download(ending):
    """Check if file ending is considered as download."""
    list = [
            'PDF',
            'DOC',
            'TXT',
            'PPT',
            'XLSX',
            'MP3',
            'SVG',
            '7Z',
            'HTML',
            'TEX',
            'MPP',
            'ODT',
            'RAR',
            'ZIP',
            'TAR',
            'EPUB',
           ]
    list_regex = [
                  'PDF'
                 ]
    if ending in list:
        return True
    for file_type in list_regex:
        if re.search(re.escape(file_type), ending):
            return True
    return False
