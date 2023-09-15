-- initialise transfer storage
CREATE TABLE ethereum.transfers_tx_storage
(
    tx_date Date,
    tx_time DateTime,
    blockchain_id UInt32,
    block UInt32,
    tx_hash_bin String,
    tx_from_bin String,
    tx_to_bin String,
    transfer_from_bin String,
    transfer_to_bin String,
    currency_id UInt32,
    entity_id UInt64,
    gas_used UInt32,
    gas_price Float32,
    gas_value Float32,
    value Float64,
    success UInt32,
    external UInt32,
    internal UInt32
)
ENGINE = MergeTree()
PARTITION BY toStartOfMonth(tx_date)
ORDER BY tx_hash_bin
SETTINGS index_granularity = 1024;

-- load e.g. cca 1 million transactions
-- run externally through the clickhouse client, replacing the path to the file:
insert into ethereum.transfers_tx_storage FROM INFILE '/home/nikola/Desktop/Ethereum/ethereum.transfers_tx_2015.tsv'
FORMAT TSV;

--set up the infrastructure for calculating the metrics as per the How to use
--(run Base.sql, Deltas.sql, Cumulative.sql in that order)

-- let's put our example here for convenience:
create table example_delete
(
    tx_date Date,
    tx_time DateTime,
    blockchain_id UInt32,
    block UInt32,
    tx_hash_bin String,
    tx_from_bin String,
    tx_to_bin String,
    transfer_from_bin String,
    transfer_to_bin String,
    currency_id UInt32,
    entity_id UInt64,
    gas_used UInt32,
    gas_price Float32,
    gas_value Float32,
    value Float64,
    success UInt32,
    external UInt32,
    internal UInt32
)
ENGINE = MergeTree()
ORDER BY tx_hash_bin;

--pick a block to fix and insert into the example_delete:
INSERT INTO example_delete
SELECT * FROM ethereum.transfers_tx_storage
WHERE block IN (
    SELECT DISTINCT block
    FROM ethereum.transfers_tx_storage
    where tx_date < today()
    -- this is to show how historical data is handled,
    -- while current day data would take the normal
    -- MV insertion route and works as usual insertion
    and block <> 0
    order by transfer_to_bin
    LIMIT 1);
--Block-level deletions and insertions only supported,
--as per the requirement. Can handle multiple blocks though.

SELECT tx_date,
       block,
       hex(transfer_to_bin) as transfer_to_hex,
       hex(transfer_from_bin) as transfer_from_hex,
       currency_id,
       value
FROM example_delete;


-- BEFORE DELETE
--******************************************************************
-- run 'store before.sql' to record the relevant derived data before the test

--delete the example block
DELETE FROM transfers_tx_storage
WHERE block IN (select distinct block from example_delete);


--run 'Detect.sql' to detect the deleted block
--and then to store the data automatically into discrepancy_log.

--discrepancy is logged:
SELECT * FROM discrepancy_log;

--run 'Delete And Recalculate.sql' to delete and reprocess the current balances,
--and historical data. Run once per every block you want to reprocess.

--fixed flag and datetime are now recorded for reference
SELECT * FROM discrepancy_log;

SELECT *, after.balance_delta-before.balance_delta
FROM aggregated_deltas_before before
LEFT JOIN aggregated_deltas_after_delete after
using (address_bin, currency_id, tx_date);

SELECT *, after.balance-before.balance
FROM current_balances_before before
LEFT JOIN current_balances_after_delete after
using (address_hex, currency_id);

SELECT *, after.balance_delta-before.balance_delta
FROM aggregated_deltas_before before
LEFT JOIN aggregated_deltas_after_delete after
using (address_bin, currency_id, tx_date);

-- run 'store after delete.sql' to record the relevant data after deletion

--prepare the 'corrected' the block data
/*ALTER TABLE example_delete UPDATE
value = value + 1000000
WHERE 1=1;
*/
--insert new data into transfers table:
insert into transfers_tx_storage
select * from example_delete;

SELECT *, after.balance_delta-before.balance_delta
FROM aggregated_deltas_before before
LEFT JOIN aggregated_deltas_after_fix after
using (address_bin, currency_id, tx_date);

SELECT *, after.balance-before.balance
FROM current_balances_before before
LEFT JOIN current_balances_after_fix after
using (address_hex, currency_id);

SELECT *, after.balance_delta-before.balance_delta
FROM aggregated_deltas_before before
LEFT JOIN aggregated_deltas_after_fix after
using (address_bin, currency_id, tx_date);

