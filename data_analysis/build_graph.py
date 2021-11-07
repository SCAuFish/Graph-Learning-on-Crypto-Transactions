import sys
import argparse


def main(transaction_file_name: str):
    transaction_graph = TransactionGraph(transaction_file=transaction_file_name)

    print(len(transaction_graph.time_series_graph))


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('-f', '--file', help='file name of the transaction file')
    opts = args.parse_args()

    main(opts.file)
