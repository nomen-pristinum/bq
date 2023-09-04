CREATE VIEW daily_balances AS
SELECT
    address_bin,
    currency_id,
    sum(balance_delta) as cumulative_balance
FROM  aggregated_deltas
WHERE aggregated_deltas.tx_date <= today() -- protection against polluted data
GROUP BY address_bin, currency_id;


SELECT
    address_bin,
    currency_id,
    sum(balance_delta) as cumulative_balance
FROM  aggregated_deltas
WHERE aggregated_deltas.tx_date <= ''-- desired date here
GROUP BY address_bin, currency_id;

SELECT
    address_bin,
    currency_id,
    sum((balance_delta)) as cumulative_balance
FROM  aggregated_deltas
WHERE aggregated_deltas.tx_date <= today() -- desired date here
GROUP BY address_bin, currency_id
ORDER BY cumulative_balance desc
LIMIT 10;

CREATE TABLE ethereum.cumulative_balances_history
(
    tx_date Date,
    address_bin String,
    currency_id UInt32,
    cumulative_balance Float64
) ENGINE = MergeTree()
ORDER BY (tx_date, address_bin, currency_id);

INSERT INTO cumulative_balances_history
SELECT
    d1.tx_date,
    d1.address_bin,
    d1.currency_id,
    sum(d2.balance_delta) as cumulative_balance
FROM ethereum.aggregated_deltas_vw d1
JOIN ethereum.aggregated_deltas_vw d2
ON d1.address_bin = d2.address_bin
AND d1.currency_id = d2.currency_id
WHERE d2.tx_date <= d1.tx_date
GROUP BY d1.tx_date, d1.address_bin, d1.currency_id
ORDER BY d1.address_bin, d1.tx_date;

CREATE MATERIALIZED VIEW ethereum.cumulative_balances_history_mv
TO ethereum.cumulative_balances_history AS
SELECT
    d1.tx_date,
    d1.address_bin,
    d1.currency_id,
    sum(d2.balance_delta) as cumulative_balance
FROM ethereum.aggregated_deltas d1 -- changes only, trigger
JOIN ethereum.aggregated_deltas_2 d2 -- full history, no trigger
ON d1.address_bin = d2.address_bin
AND d1.currency_id = d2.currency_id
WHERE d2.tx_date <= d1.tx_date
GROUP BY d1.tx_date, d1.address_bin, d1.currency_id;

/*CREATE VIEW ethereum.cumulative_balances_history_vw
AS
SELECT
    tx_date,
    address_bin,
    currency_id,
    sumMerge(cumulative_balance) as cumulative_balance
FROM cumulative_balances_history
GROUP BY tx_date, address_bin, currency_id;*/

SELECT
    *
FROM daily_balances
WHERE cumulative_balance <0;