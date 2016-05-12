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

"""Recommender to calculate the recommendation scores."""

from __future__ import absolute_import, print_function

import gc
from array import array
from collections import defaultdict

import networkx as nx
import pandas as pd


class GraphRecommender(object):
    """Recommender which recommends records based on a graph structure."""

    def __init__(self, storage, settings=None):
        """Constructor."""
        self.storage = storage
        self._graph = nx.Graph()
        self.statistics = {}
        self.all_records = defaultdict(int)

    def recommend_for_record(self, record_id, depth=4, num_reco=10):
        """Calculate recommendations for record."""
        data = calc_scores_for_node(self._graph, record_id, depth, num_reco)
        return data.Node.tolist(), data.Score.tolist()

    def load_profile(self, profile_name):
        """Load user profiles from file."""
        data = self.storage.get_user_profiles(profile_name)

        for x in data.get_user_views():
            self._graph.add_edge(int(x[0]), int(x[1]), {'weight': float(x[2])})
            self.all_records[int(x[1])] += 1

        return self._graph

    def del_big_nodes(self, grater_than=215):
        """Delete big nodes with many connections from the graph."""
        G = self._graph
        it = G.nodes_iter()
        node_paths = []
        node_names = []
        del_nodes = []
        summe = 1
        count = 1
        for node in it:
            l = len(G[node])
            if l > grater_than:
                del_nodes.append(node)
                continue
            summe += l
            node_names.append(node)
            node_paths.append(l)
            count += 1
        for node in del_nodes:
            G.remove_node(node)
            if node > 1000000000:
                self.valid_user.pop(node)

        print("Nodes deleted: {}".format(len(del_nodes)))


def calc_scores_for_node(G, node, depth_limit=22,
                         number_of_recommendations=None, impact_mode=10):
    """Calculate the score of multiple records."""
    n, w, dep, _ = dfs_edges(G, node, depth_limit, "Record")
    count_total_ways = len(n)
    # print "Number of paths {}".format(len(n))
    if impact_mode == 0:
        impact_div = 12
    elif impact_mode == 1:
        impact_div = 1000
    elif impact_mode == 2:
        impact_div = 100
    elif impact_mode == 10:
        impact_div = count_total_ways
    elif impact_mode == 11:
        impact_div = count_total_ways/2

    d_ = {'Nodes': n, 'Scores': w, 'Depth': dep}
    d = pd.DataFrame(data=d_)
    del n, w, dep, d_
    n, w, dep = None, None, None
    gc.collect()

    nodes = array('I')
    weight_high = array('f')
    weight_new = array('f')
    ways = array('I')
    nodes_with_weight = d.groupby('Nodes')

    del d
    gc.collect()
    # print "Number nodes {}".format(len(nodes_with_weight))
    for node, end_nodes in nodes_with_weight:
        nodes.append(node)
        new_score, highest_score, number_of_paths = \
            calc_weight_of_multiple_paths(end_nodes, impact_div)
        weight_high.append(highest_score)
        weight_new.append(new_score)
        ways.append(number_of_paths)

    new_weights_d = {'Node': nodes, 'Score_Highest': weight_high,
                     'Score': weight_new, 'Paths': ways}
    new_weights = pd.DataFrame(data=new_weights_d)
    del new_weights_d, nodes, weight_high, weight_new, ways
    gc.collect()
    # Numpy sort by score
    new_weights = new_weights.sort_values(by='Score', ascending=False)
    new_weights = new_weights[:number_of_recommendations]

    return new_weights


def dfs_edges(G, start, depth_limit=1, get_only=True, get_path=False):
    """Deepest first search."""
    depth_limit = depth_limit - 1

    # creates unsigned int array (2 Byte)
    output_nodes = array('L')
    output_depth = array('I')
    # creates float array (4 Byte)
    output_weights = array('f')
    apath = []

    if G.node.get(start) is None:
        # raise KeyError('Start node not found')
        print('Start node not found')
        return output_nodes, output_weights, output_depth, apath

    visited = set()
    visited.add(start)

    # Save the start node with its data to the stack
    stack = [(start, G.edges_iter(start, data=True), 1.0)]
    visited.add(start)
    while stack:
        if len(output_nodes) > 80100100:
            print("To many nodes for: {}".format(start))
            del output_nodes
            del output_weights
            del output_depth
            output_nodes = array('L')
            output_depth = array('I')
            # creates float array (4 Byte)
            output_weights = array('f')
            gc.collect()
            break
        parent, children, weight = stack[-1]
        try:
            parent_, child, child_keys = next(children)
            # print "child: {}, parent_data: {}".format(child, parent_data)
            if child not in visited:
                weight = child_keys.get('weight', 1.0) * weight
                visited.add(child)

                if len(stack) >= depth_limit or weight <= 0.00001:
                    visited.remove(child)
                else:
                    stack.append((child, G.edges_iter(child, data=True),
                                 weight))

                # if its not and user.
                if get_only and child > 100000000000:
                    # if get_only and G.node[child].get('Type') != get_only:
                    continue

                output_nodes.append(child)
                output_weights.append(weight)
                output_depth.append(len(stack))
                if get_path:
                    apath.append([step[0] for step in stack])
        except StopIteration:
            stack.pop()
            visited.remove(parent)
            # if data.get('Type') == "Node":

    return output_nodes, output_weights, output_depth, apath


def calc_weight_of_multiple_paths(path_scores, impact_div=12):
    """Caluculate the weight of multipe paths."""
    number_of_paths = len(path_scores)
    if number_of_paths > 1:
        score_total = 0.0
        highest_score = 0.0
        for score in path_scores.Scores:
            score_total += score
            if highest_score < score:
                highest_score = score

        score_mean = score_total / number_of_paths
        # print "score_total: {}".format(score_total)
        # print "score_mean: {}".format(score_mean)
        # print "number_of_paths: {}".format(number_of_paths)

        # Calculate the weight depending on how many ways are found
        weight_count_impact = number_of_paths / float(number_of_paths +
                                                      impact_div)
        # print "weight_count_impact: {}".format(weight_count_impact)

        new_score = highest_score + ((1 + weight_count_impact) * score_mean)
        # print "new_score: {}".format(new_score)

        return new_score, highest_score, number_of_paths

    else:
        return (path_scores.Scores.iloc[0], path_scores.Scores.iloc[0],
                number_of_paths)
