from graph_tools.components.graph import GraphGenerator, TransactionGraph


def shrink_by_least_transaction(original_file, shrunk_file, least_transaction: int = 1, required_full_edge: bool = False):
    """
    shrink the original transaction file, with requirement on the mininum number of transactions of a given wallet
    :param original_file: input file to be shrunk
    :param shrunk_file: output file to write to
    :param least_transaction: the requirement of mininum transaction number
    :param required_full_edge: whether both ends of a transaction need to meed the min-trans requirement or not
    :return:
    """
    generator = GraphGenerator(original_file)
    wallet_freq = generator.wallet_frequency

    wallets_to_keep = filter(lambda f2w: f2w[0] >= least_transaction, wallet_freq)
    wallets_to_keep = set(map(lambda f2w: f2w[1], wallets_to_keep))

    with open(original_file, 'r') as original_reader, open(shrunk_file, 'w') as shrunk_writer:
        for i, line in enumerate(original_reader.readlines()):
            if i == 0:
                # copy the original line
                shrunk_writer.write(line)

            from_add, to_add, _, _  = line.strip().split(',')
            if required_full_edge:
                if from_add in wallets_to_keep and to_add in wallets_to_keep:
                    shrunk_writer.write(line)
            else:
                if from_add in wallets_to_keep or to_add in wallets_to_keep:
                    shrunk_writer.write(line)
