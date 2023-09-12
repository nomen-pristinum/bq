
SELECT * FROM discrepancy_log;

DELETE FROM balance_deltas
WHERE (address_bin, currency_id) IN (
    SELECT distinct address_bin, currency_id
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix)
AND tx_time >= (
    SELECT distinct block_time
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix
    LIMIT 1 --safety
    ) ;

DELETE FROM aggregated_deltas
WHERE (address_bin, currency_id) IN (
    SELECT distinct address_bin, currency_id
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix)
AND tx_date = (SELECT toDate(block_time)
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix);

INSERT INTO ethereum.aggregated_deltas
SELECT
    tx_date,
    address_bin,
    currency_id,
    value as balance_delta
FROM ethereum.balance_deltas
WHERE (address_bin, currency_id) IN (
    SELECT distinct address_bin, currency_id
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix)
AND tx_date = (SELECT toDate(block_time)
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix);

TRUNCATE TABLE current_balances;

INSERT INTO current_balances
SELECT
    address_bin,
    currency_id,
    sum(balance_delta) as balance
FROM aggregated_deltas
GROUP BY address_bin, currency_id;

DELETE FROM cumulative_balances_history
WHERE (address_bin, currency_id) IN (
    SELECT distinct address_bin, currency_id
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix)
AND tx_date >=  -- everything is polluted
                -- for the affected addresses
                -- past this point in time:
    (SELECT toDate(block_time)
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix);

INSERT INTO cumulative_balances_history
SELECT
    d1.tx_date,
    d1.address_bin,
    d1.currency_id,
    sum(d2.balance_delta) AS cumulative_balance
FROM ethereum.aggregated_deltas d1
JOIN ethereum.aggregated_deltas d2
ON d1.address_bin = d2.address_bin
AND d1.currency_id = d2.currency_id
WHERE d2.tx_date <= d1.tx_date
AND d1.tx_date >= (
    SELECT toDate(block_time)
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix)
AND (d1.address_bin, d1.currency_id) IN (
    SELECT distinct address_bin, currency_id
    FROM discrepancy_log
    WHERE fixed_flag = 0
    AND block = next_block_to_fix)
GROUP BY d1.tx_date, d1.address_bin, d1.currency_id
ORDER BY d1.address_bin, d1.tx_date;


ALTER TABLE discrepancy_log
UPDATE fixed_flag = 1
WHERE block = next_block_to_fix;

ALTER TABLE discrepancy_log
UPDATE next_block_to_fix = (
    SELECT min(block)
    FROM discrepancy_log
    WHERE fixed_flag = 0
)
WHERE 1 = 1;



