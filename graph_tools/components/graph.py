"""
Define graph class
"""
from collections import defaultdict
from typing import Callable
from networkx import Graph, DiGraph
import tqdm


class TransactionGraph:
    def __init__(self, *, transaction_file=None, total_time_steps: int = None):
        self.time_series_graph = dict()
        if transaction_file is not None:
            self._construct_from_file(transaction_file, total_time_steps)

    def _construct_from_file(self, transaction_file, total_time_steps: int = None):
        graph_generator = GraphGenerator(transaction_file)
        all_time_steps = graph_generator.time_steps
        if total_time_steps is not None:
            all_time_steps = all_time_steps[:total_time_steps]

        for time_step in tqdm.tqdm(all_time_steps):
            self.time_series_graph[time_step] = DiGraph()
            self.time_series_graph[time_step].add_edges_from(graph_generator.generate_edges(self.get_filter(time_step)))

    def get_filter(self, specified_time_step: int):
        def should_exclude(from_address: str, to_address: str, amount: float, time_step: int):
            return time_step != specified_time_step

        return should_exclude

class GraphGenerator:
    def __init__(self, transaction_file):
        self.transaction_file = transaction_file
        self.time_steps = self._all_time_steps()
        self.wallet_frequency = self._wallet_frequency()
        self.address2idx = dict()

    def generate_edges(self, should_exclude: Callable[[str, str, float, int], bool] = None):
        with open(self.transaction_file, 'r') as transaction_reader:
            for i, line in enumerate(transaction_reader.readlines()):
                if i == 0:
                    # skip header line
                    continue

                from_address, to_address, amount, trans_time_step = line.strip().split(',')
                if should_exclude(from_address, to_address, float(amount), int(trans_time_step)):
                    continue

                if from_address not in self.address2idx:
                    self.address2idx[from_address] = len(self.address2idx)
                if to_address not in self.address2idx:
                    self.address2idx[to_address] = len(self.address2idx)

                yield self.address2idx[from_address], self.address2idx[to_address], {'amount': amount}

    def _all_time_steps(self) -> list:
        all_time_steps = set()
        with open(self.transaction_file, 'r') as transaction_reader:
            for i, line in enumerate(transaction_reader.readlines()):
                if i == 0:
                    continue

                _, _, _, time_step_str = line.strip().split(',')
                all_time_steps.add(int(time_step_str))

        return sorted(list(all_time_steps))

    def _wallet_frequency(self):
        """
        Get the most frequent appearing n wallet addresses in a given range of time_steps. Or find across whole file,
        if time_step is None
        :param n: the top number of address to use
        :param time_step: the time steps to pick from
        :return:
        """
        address_count = defaultdict(int)
        with open(self.transaction_file, 'r') as transaction_reader:
            for i, line in enumerate(transaction_reader.readlines()):
                if i == 0:
                    continue

                from_address, to_address, _, _ = line.strip().split(',')
                address_count[from_address] += 1
                address_count[to_address] += 1

        address_count = [(count, address) for address, count in address_count.items()]
        return sorted(address_count)
