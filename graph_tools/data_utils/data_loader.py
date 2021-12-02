"""
Load data from full file into a graph. Could be revised for better effiency and adaptability
"""
from networkx import MultiDiGraph
import random

from graph_tools.components.graph import GraphGenerator

class SingleGraphLoader:
    """
    Load a single graph each time from the file
    """
    def __init__(self, transaction_csv_file, *,
                 random_seed=7, k_hop=5, time_step_interval=168, incoming_sampling=5, outgoing_sampling=5):
        """

        :param transaction_csv_file:
        :param price_file:
        :param balance_file:
        :param k_hop:
        :param time_step_interval: default is 168, which is 7 * 24 = 1 week of data
        """
        self.trasaction_csv_file = transaction_csv_file

        # hyperparams when loading
        self.random_seed = random_seed
        if self.random_seed is not None:
            random.seed(self.random_seed)
        self.k_hop = k_hop
        self.time_step_interval = time_step_interval
        self.incoming_sampling=5
        self.outgoing_sampling=5

        # Generate a graph that includes all nodes and edges
        self._graph = MultiDiGraph()
        self._graph_generator = GraphGenerator(transaction_csv_file)
        self._graph.add_edges_from(self._graph_generator.generate_edges(lambda src, tgt, amt, t: False))
        print(f"Full graph contains {len(self._graph.nodes)} nodes and {len(self._graph.edges)} edges")

    def __iter__(self):
        for node, _ in self._graph.nodes.items():
            predecessors = self._graph.predecessors(node)
            predecessors = list(predecessors)
            if len(predecessors) == 0:
                continue

            predecessor = random.choice(predecessors)
            time_steps = []
            for k, v in self._graph.get_edge_data(predecessor, node).items():
                time_steps.append(v['time_step'])

            for time_step in time_steps:
                time_step = random.choice(time_steps)

                edges = []
                visited = set()
                self._find_edge_bfs({node}, edges, visited, time_step, k_hop=self.k_hop)

                sampled_graph = MultiDiGraph()
                sampled_graph.add_edges_from(edges)

                yield sampled_graph

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

        self._find_edge_bfs(new_frontier, edges_to_populate, visited, sampled_time_step, k_hop-1)