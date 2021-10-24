-- Reduce the number of block into time step: 240 blocks is 1 time step => 240x15s = 1 hour is 1 time step
-- Since it is hard to accumulate and calculate the balance of an account, will use the total amount of transactions 
-- in 'memory' to simulate its balance
-- Memory length is 4320 (24 * 60 * 30 * 6 / 60) time steps, which is the number of time steps in 180 days
DECLARE BLOCKS_PER_STEP INT64 DEFAULT 240;
DECLARE STEPS_IN_BUFFER INT64 DEFAULT 4320;
DECLARE WEI_IN_GWEI DEFAULT 1000000000;
DECLARE WEI_IN_ETHER DEFAULT 1000000000000000000;
-- Reduce number of blocks by compressing 10 blocks into 1
WITH transactions AS (
    SELECT from_address, to_address, value, gas, gas_price, receipt_gas_used, receipt_status, block_timestamp,
        cast(block_number/BLOCKS_PER_STEP AS int) AS time_step, 
        cast(block_number/(STEPS_IN_BUFFER * BLOCKS_PER_STEP) AS int) AS buffer_step,
        transaction_type
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE receipt_status IS NOT NULL AND receipt_status=1 AND value > WEI_IN_ETHER
)
SELECT MIN(block_timestamp) AS start_time, MAX(block_timestamp) AS end_time, 
	MIN(gas_price) AS min_gas_price, MAX(gas_price) AS max_gas_price, AVG(gas_price) AS avg_gas_price,
	time_step
FROM transactions
GROUP BY time_step
;