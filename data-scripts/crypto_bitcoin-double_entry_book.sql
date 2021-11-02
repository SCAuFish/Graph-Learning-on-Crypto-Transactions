-- Query for transaction information related to addresses with top 1000 balance.
-- Some questions about this query (by Cheng):
-- 1. I notice that for each transaction, it may relate to one or multiple input addresses and output addresses. 
-- This makes balance tracking more complicated. A user may control multiple addresses, do we want to treat each address as a node, or cluster them together
-- and treat them all as a node?
-- 2. In the current definition of `double_entry_book`, we concate multiple addresses together into a string. 
-- This means that transactions made from [address_1, address_2] will be treated separately from [address_1], while they are highly associated.
-- 3. We might need a more complicated way to calculate the balance of each node through out the time. Data in `transactions` table basically records the delta
-- between time steps. We have to accumulate this delta together, as we do when defining `double_entry_book` table to get current balance, and this is not trivial.

WITH double_entry_book AS (
   -- debits
   SELECT array_to_string(inputs.addresses, ",") as address, inputs.type, -inputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.inputs` as inputs
   UNION ALL
   -- credits
   SELECT array_to_string(outputs.addresses, ",") as address, outputs.type, outputs.value as value
   FROM `bigquery-public-data.crypto_bitcoin.outputs` as outputs
),
top_1000_addresses AS (
    SELECT address, sum(value) as balance
    FROM double_entry_book
    GROUP BY address
    ORDER BY balance DESC
    LIMIT 1000
)
SELECT
  transactions.hash,
  transactions.block_timestamp,
  transactions.input_value,
  inputs.addresses as credit_addresses,
  outputs.addresses as debit_address
FROM
  `bigquery-public-data.crypto_bitcoin.transactions` as transactions,
  transactions.outputs as outputs,
  transactions.inputs as inputs
WHERE
    array_to_string(inputs.addresses, ",") in (SELECT address FROM top_1000_addresses) AND array_to_string(outputs.addresses, ",") in (SELECT address FROM top_1000_addresses)
LIMIT
  100;