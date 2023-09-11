--TO-DO
--SummingMT current balance table
--MV inserting into it from agg deltas WHERE date = today()
--INSERT to initialise it
--VW pulling from the table final SUM()
--MV inserting its changes into Cumulative History table
--Cumulative History becomes ReplacingMT
--stop using aggregated delta 2, not needed with this approach
--do use finalising SUM() when pulling from aggregations
--all this to handle regular business
--another MV for historical data fixes
--  WHERE < today(),
--  Cumulative History JOIN Changes
--  USING date, currency, address
--  insert change+old value

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
WHERE aggregated_deltas.tx_date <= '2015-12-12'-- desired date here
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
) ENGINE = ReplacingMergeTree(cumulative_balance)
ORDER BY (tx_date, address_bin, currency_id);

/* initial load, run if there is existing data in transfers_tx_storage */
INSERT INTO cumulative_balances_history
SELECT
    d1.tx_date,
    d1.address_bin,
    d1.currency_id,
    sum(d2.balance_delta) as cumulative_balance
FROM (
    SELECT
        tx_date,
        address_bin,
        currency_id,
        sum(balance_delta) as cumulative_balance
    FROM ethereum.aggregated_deltas
    group by tx_date, address_bin, currency_id) d1
    JOIN ethereum.aggregated_deltas d2
                    ON d1.address_bin = d2.address_bin
                        AND d1.currency_id = d2.currency_id
      WHERE d2.tx_date <= d1.tx_date d1
JOIN ethereum.aggregated_deltas d2
ON d1.address_bin = d2.address_bin
AND d1.currency_id = d2.currency_id
WHERE d2.tx_date <= d1.tx_date
GROUP BY d1.tx_date, d1.address_bin, d1.currency_id
ORDER BY d1.tx_date, d1.address_bin, d1.currency_id;

CREATE MATERIALIZED VIEW ethereum.cumulative_balances_history_mv
TO ethereum.cumulative_balances_history AS
SELECT
    d1.tx_date,
    d1.address_bin,
    d1.currency_id,
    sum(d2.balance_delta) as cumulative_balance
FROM ethereum.aggregated_deltas d1 -- changes only, trigger
JOIN ethereum.aggregated_deltas_2 d2 -- full change history, no trigger
ON d1.address_bin = d2.address_bin
AND d1.currency_id = d2.currency_id
WHERE d2.tx_date <= d1.tx_date
GROUP BY d1.tx_date, d1.address_bin, d1.currency_id;



CREATE VIEW ethereum.cumulative_balances_history_vw
AS
select *
from (
    SELECT tx_date,
         address_bin,
         currency_id,
         cumulative_balance,
         max(tx_date) over (
             partition by address_bin, currency_id)
             as last_balance_date
    FROM (
        select tx_date,
               address_bin,
               currency_id,
               sum(cumulative_balance) as cumulative_balance
        from cumulative_balances_history
        group by tx_date, address_bin, currency_id)
    where tx_date <= today()
)
where tx_date = last_balance_date
;


SELECT
    *
FROM daily_balances
WHERE cumulative_balance <0;