from torch_geometric.data import HeteroData
from tqdm import tqdm
import torch

def sort_by_time_step(input_csv_file):
  rows = []
  with open(input_csv_file, 'r') as reader:
    for i, line in enumerate(reader):
      if i == 0:
        continue

      from_addr, to_addr, amount, time_step, price, _, countnode_seller,countbuy_seller,countsell_seller,countnode_buyer,countbuy_buyer,countsell_buyer = line.split(',')
      amount = float(amount)
      time_step = int(time_step)
      price = float(price)
      countnode_seller,countbuy_seller,countsell_seller,countnode_buyer,countbuy_buyer,countsell_buyer = \
        [int(var) for var in [countnode_seller,countbuy_seller,countsell_seller,countnode_buyer,countbuy_buyer,countsell_buyer]]
      rows.append([from_addr, to_addr, amount, time_step, price, countnode_seller,countbuy_seller,countsell_seller,countnode_buyer,countbuy_buyer,countsell_buyer])

    return sorted(rows, key=lambda t: t[3])


def load_hetero_data(csv_file, node_attr_dim=8):
  global_graph_data = HeteroData()

  addr_time_step_to_index = dict()
  idx_to_features = dict()
  latest_addr_appearnce = dict()
  same_addr_relations = dict()
  transfer_to_relation = dict()
  idx_to_addr = dict()
  addr_node_count = dict()
  current_idx = 0
  rows = sort_by_time_step(csv_file)
  for i, (from_addr, to_addr, amount, time_step, price, countnode_seller,countbuy_seller,countsell_seller,countnode_buyer,countbuy_buyer,countsell_buyer) in tqdm(enumerate(rows)):
    if i == 0:
      continue

    from_addr_time_step = (from_addr, time_step)
    to_addr_time_step = (to_addr, time_step)
    addr_node_count[from_addr] = countnode_seller
    addr_node_count[to_addr] = countnode_buyer

    # get index for the (addr, time_step) pair
    if from_addr_time_step not in addr_time_step_to_index:
      addr_time_step_to_index[from_addr_time_step] = current_idx
      idx_to_addr[current_idx] = from_addr
      current_idx += 1
    if to_addr_time_step not in addr_time_step_to_index:
      addr_time_step_to_index[to_addr_time_step] = current_idx
      idx_to_addr[current_idx] = to_addr
      current_idx += 1
    from_addr_idx = addr_time_step_to_index[from_addr_time_step]
    to_addr_idx = addr_time_step_to_index[to_addr_time_step]

    # update node features
    if from_addr_idx in idx_to_features:
      in_amount, out_amount, pre_price, pre_time_step, countnode_seller,countbuy_seller,countsell_seller = idx_to_features[from_addr_idx]
      assert pre_price == price
      assert pre_time_step == time_step
      idx_to_features[from_addr_idx] = (in_amount, out_amount + amount, price, time_step, countnode_seller,countbuy_seller,countsell_seller)
    else:
      idx_to_features[from_addr_idx] = (0, amount, price, time_step, countnode_seller,countbuy_seller,countsell_seller)
    if to_addr_idx in idx_to_features:
      in_amount, out_amount, pre_price, pre_time_step, countnode_buyer,countbuy_buyer,countsell_buyer = idx_to_features[to_addr_idx]
      assert pre_price == price, f'previously: {pre_price}; current {price}'
      assert pre_time_step == time_step
      idx_to_features[to_addr_idx] = (in_amount + amount, out_amount, price, time_step, countnode_buyer,countbuy_buyer,countsell_buyer)
    else:
      idx_to_features[to_addr_idx] = (amount, 0, price, time_step, countnode_buyer,countbuy_buyer,countsell_buyer)

    # add transfer-to relation
    transfer_to_relation[(from_addr_idx, to_addr_idx)] \
      = {'amount': amount, 'time_step': time_step, 'price': price}

    # add same-as relation
    if from_addr in latest_addr_appearnce:
      latest_time_step = latest_addr_appearnce[from_addr]
      assert latest_time_step <= time_step, f"latest: {latest_time_step}, curr: {time_step}"
      latest_idx = addr_time_step_to_index[(from_addr, latest_time_step)]
      if latest_idx != from_addr_idx:
        same_addr_relations[
          (latest_idx,from_addr_idx)
          ] = {'amount': amount, 'time_step': time_step, 'price': price}
    latest_addr_appearnce[from_addr] = time_step
    if to_addr in latest_addr_appearnce:
      latest_time_step = latest_addr_appearnce[to_addr]
      assert latest_time_step <= time_step, f"latest: {latest_time_step}, curr: {time_step}"
      latest_idx = addr_time_step_to_index[(to_addr, latest_time_step)]
      if latest_idx != to_addr_idx:
        same_addr_relations[
          (latest_idx,to_addr_idx)
          ] = {'amount': amount, 'time_step': time_step, 'price': price}
    latest_addr_appearnce[to_addr] = time_step

  node_count = len(addr_time_step_to_index.keys())
  same_addr_relation_count = len(same_addr_relations.keys())
  transfer_to_relation_count = len(transfer_to_relation.keys())
  all_edges = set(same_addr_relations.keys()).union(set(transfer_to_relation.keys()))
  has_relation_count = len(all_edges)
  print(same_addr_relation_count, transfer_to_relation_count, has_relation_count)

  global_graph_data['wallet'].x = torch.randn(node_count, node_attr_dim)
  global_graph_data['wallet', 'is_parent_of', 'wallet'].edge_index = \
    torch.zeros((2, same_addr_relation_count), dtype=torch.long)
  global_graph_data['wallet', 'is_child_of', 'wallet'].edge_index = \
    torch.zeros((2, same_addr_relation_count), dtype=torch.long)
  global_graph_data['wallet', 'transfers_to', 'wallet'].edge_index = \
    torch.zeros((2, transfer_to_relation_count), dtype=torch.long)
  global_graph_data['wallet', 'transferred_from', 'wallet'].edge_index = \
    torch.zeros((2, transfer_to_relation_count), dtype=torch.long)
  global_graph_data['wallet', 'has_relation_with', 'wallet'].edge_index = \
    torch.zeros((2, has_relation_count * 2), dtype=torch.long)
    
  # populate node features
  for i in range(node_count):
    features = idx_to_features[i]
    global_graph_data['wallet'].x[i, :node_attr_dim] = torch.tensor(idx_to_features[i])

  # populate edge_idx for same-as relation
  for i, (edge, feature) in tqdm(enumerate(same_addr_relations.items())):
    global_graph_data['wallet', 'is_parent_of', 'wallet'].edge_index[:, i] = \
      torch.tensor([edge[0], edge[1]])

  # populate edge_idx for transfer-to relation
  for i, (edge, feature) in tqdm(enumerate(transfer_to_relation.items())):
    global_graph_data['wallet', 'transfers_to', 'wallet'].edge_index[:, i] = \
      torch.tensor([edge[0], edge[1]])
  return global_graph_data, addr_time_step_to_index, \
    same_addr_relations, transfer_to_relation, latest_addr_appearnce, idx_to_addr, addr_node_count
    