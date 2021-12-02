# 1. reference to Karate Dataset implementation: https://pytorch-geometric.readthedocs.io/en/latest/_modules/torch_geometric/datasets/karate.html#KarateClub
# 2. dataset creation tutorial: https://pytorch-geometric.readthedocs.io/en/latest/notes/create_dataset.html

import torch
import numpy as np
import pandas as pd
from networkx import MultiDiGraph
from torch_geometric.data import InMemoryDataset, Data

from graph_tools.components.graph import GraphGenerator
from graph_tools.data_utils.data_loader import SingleGraphLoader


class TransactionGraphs(InMemoryDataset):
    def __init__(self, csv_file_name, price_file_name, time_conversion_file_name, *,
                 k_hop=5, time_step_interval=168, price_prediction_interval=24,
                 incoming_sampling=5, outgoing_sampling=5):
        """
        k_hop: size of the sub-graph
        time_step_interval: time period whose transactions will be included, 168=7*24 is a week
        price_prediction_interval: the time to verify profitability, 24 is a day
        incoming_sampling: maximum number of incoming edges, to avoid hub-node
        outgoing_sampling: maximum number of outgoing edges, to avoid hub-node
        """
        super(TransactionGraphs, self).__init__()
        self.csv_file_name = csv_file_name
        file_name, csv_extension = os.path.splitext(os.path.basename(csv_file_name))
        self.processed_file_name = f'{os.path.dirname(csv_file_name)}/processed_{file_name}'

        self.price_file_name = price_file_name
        self.time_conversion_file_name = time_conversion_file_name

        # hyperparameters to generate sub-graphs
        self.k_hop = k_hop
        self.time_step_interval = time_step_interval
        self.price_prediction_interval = price_prediction_interval
        self.incoming_sampling = incoming_sampling
        self.outgoing_sampling = outgoing_sampling

        # load processed data
        for processed_file in self.processed_file_names:
            if not os.path.exists(processed_file):
                self.preprocess()
            else:
                print("skipped pre-processing")
        self.data, self.slices = torch.load(self.processed_file_names[0])

    @property
    def raw_file_names(self):
        return [self.csv_file_name]

    @property
    def processed_file_names(self):
        return [self.processed_file_name]

    def preprocess(self):
        # generate unit ego graph, and collate into self.data
        full_graph_loader = \
            SingleGraphLoader(self.csv_file_name, self.price_file_name,
                              self.time_conversion_file_name,
                              k_hop=self.k_hop,
                              time_step_interval=self.time_step_interval,
                              incoming_sampling=self.incoming_sampling,
                              outgoing_sampling=self.outgoing_sampling)

        graph_data = []
        for ego_graph, time_step in full_graph_loader:
            future_price_time_step = time_step + self.price_prediction_interval
            if future_price_time_step > full_graph_loader.max_time_step:
                continue
            future_price = full_graph_loader.find_price(future_price_time_step)
            # construct edge-indices and node features from the networkx graph ego_graph
            ego_graph_data = Data(
                x=None,
                edge_index=None,
                edge_attr=None,
                y=torch.tensor([future_price])
            )
            graph_data.append(ego_graph_data)

        data, slices = self.collate(graph_data)
        torch.save((data, slices), self.processed_file_names[0])

    def _find_edge_bfs(self, frontier, edges_to_populate, visited, sampled_time_step, k_hop):
        if k_hop == 0 or len(frontier) == 0:
            return

        new_frontier = set()
        # Add at most 10 incoming/outgoing edges for each node in the frontier
        for node in frontier:
            if node in visited:
                continue
            visited.add(node)
            incoming_edge_count = 0

            incoming_candidate_edges = []
            for predecessor in self._graph.predecessors(node):
                for idx, edge_info in self._graph.get_edge_data(predecessor, node).items():
                    if edge_info['time_step'] + self.time_step_interval >= sampled_time_step and \
                            edge_info['time_step'] <= sampled_time_step:
                        if predecessor in visited:
                            continue
                        visited.add(predecessor)
                        incoming_candidate_edges.append((predecessor, edge_info))

            if len(incoming_candidate_edges) > self.incoming_sampling:
                incoming_candidate_edges = random.choices(incoming_candidate_edges, k=self.incoming_sampling)

            outgoing_candidate_edges = []
            for successor in self._graph.successors(node):
                for idx, edge_info in self._graph.get_edge_data(node, successor).items():
                    if edge_info['time_step'] + self.time_step_interval >= sampled_time_step and \
                            edge_info['time_step'] <= sampled_time_step:
                        if successor in visited:
                            continue
                        visited.add(successor)
                        outgoing_candidate_edges.append((successor, edge_info))

            if len(outgoing_candidate_edges) > self.outgoing_sampling:
                outgoing_candidate_edges = random.choices(outgoing_candidate_edges, k=self.outgoing_sampling)

            for (predecessor, edge_info) in incoming_candidate_edges:
                edges_to_populate.append((predecessor, node, edge_info))
                new_frontier.add(predecessor)

            for (successor, edge_info) in outgoing_candidate_edges:
                edges_to_populate.append((node, successor, edge_info))
                new_frontier.add(successor)

        self._find_edge_bfs(new_frontier, edges_to_populate, visited, sampled_time_step, k_hop - 1)
