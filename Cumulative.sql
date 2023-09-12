--TO-DO
--SummingMT current balance table
CREATE TABLE current_balances
(
    address_bin String,
    currency_id UInt32,
    balance Float64
) ENGINE = SummingMergeTree(balance)
ORDER BY (address_bin, currency_id);

--INSERT to initialise current balances
INSERT INTO current_balances
SELECT
    address_bin,
    currency_id,
    sum(balance_delta) as balance
FROM aggregated_deltas
GROUP BY address_bin, currency_id;

--MV inserting into it FROM balance deltas WHERE date = today()
CREATE MATERIALIZED VIEW current_balances_mv
to current_balances
as
SELECT
    address_bin,
    currency_id,
    value as balance
FROM balance_deltas;

--VW pulling the final SUM() FROM balances
CREATE VIEW current_balances_vw
AS
SELECT
    address_bin,
    currency_id,
    sum(balance) as balance
FROM current_balances
GROUP BY address_bin, currency_id;

CREATE VIEW top_holders_vw
as
SELECT *
FROM current_balances
ORDER BY balance desc
LIMIT 100;

--Cumulative History becomes ReplacingMT
--stop using aggregated delta 2, not needed with this approach
--do use finalising SUM() when pulling FROM aggregated deltas
--all this to handle regular business
CREATE TABLE ethereum.cumulative_balances_history
(
    tx_date Date,
    address_bin String,
    currency_id UInt32,
    cumulative_balance Float64
) ENGINE = ReplacingMergeTree(cumulative_balance)
ORDER BY (tx_date, address_bin, currency_id);

--MV inserting the changes of current balances into Cumulative History table
CREATE MATERIALIZED VIEW cumulative_balances_history_mv
TO cumulative_balances_history AS
select today() AS tx_date,
       address_bin,
       currency_id,
       cb.balance --already updated
           AS cumulative_balance
from ( --just a trigger ensuring 1-1 join
    SELECT address_bin,
        currency_id,
        sum(value) as daily_change
    FROM balance_deltas
    WHERE tx_date = today()
    GROUP BY  address_bin, currency_id)
JOIN current_balances_vw cb
USING (address_bin, currency_id);

--another MV for historical data fixes
CREATE MATERIALIZED VIEW cumulative_balances_history_fix_mv
TO cumulative_balances_history
AS
SELECT tx_date,
       address_bin,
       currency_id,
       sum(cumulative_balance/cnt + daily_change)
           as cumulative_balance
FROM (
    SELECT tx_date,
        address_bin,
        currency_id,
        cumulative_balance,
        daily_change,
        count(*) OVER (
            PARTITION BY tx_date, address_bin, currency_id)
            AS cnt
    FROM (
        SELECT tx_date,
            address_bin,
            currency_id,
            sum(value) as daily_change
        FROM ethereum.balance_deltas
        WHERE tx_date < today()
        GROUP BY tx_date, address_bin, currency_id) ch
        --changes only, trigger
    JOIN cumulative_balances_history h
    ON ch.address_bin = h.address_bin
    AND ch.currency_id = h.currency_id
    WHERE ch.tx_date <= h.tx_date
)
GROUP BY tx_date, address_bin, currency_id;


/* initial load, run if there is existing data in transfers_tx_storage */
INSERT INTO cumulative_balances_history
SELECT
    d1.tx_date,
    d1.address_bin,
    d1.currency_id,
    sum(d2.balance_delta) AS cumulative_balance
FROM ( --surprisingly small performance loss here:
    SELECT
        tx_date,
        address_bin,
        currency_id,
        sum(balance_delta) as balance_delta
    FROM ethereum.aggregated_deltas
    GROUP BY tx_date, address_bin, currency_id) d1
JOIN (
    SELECT
        tx_date,
        address_bin,
        currency_id,
        sum(balance_delta) as balance_delta
    FROM ethereum.aggregated_deltas
    GROUP BY tx_date, address_bin, currency_id) d2
ON d1.address_bin = d2.address_bin
AND d1.currency_id = d2.currency_id
WHERE d2.tx_date <= d1.tx_date
GROUP BY d1.tx_date, d1.address_bin, d1.currency_id
ORDER BY d1.tx_date, d1.address_bin, d1.currency_id;


CREATE VIEW ethereum.cumulative_balances_history_vw
AS
SELECT *
FROM (
    SELECT tx_date,
         address_bin,
         currency_id,
         cumulative_balance,
         max(tx_date) OVER (
             PARTITION BY address_bin, currency_id)
             AS last_balance_date
    FROM cumulative_balances_history FINAL /*(
        SELECT tx_date,
               address_bin,
               currency_id,
               sum(cumulative_balance) as cumulative_balance
        FROM cumulative_balances_history
        GROUP BY tx_date, address_bin, currency_id)*/
    WHERE tx_date <= today()
)
WHERE tx_date = last_balance_date
;


SELECT
    *
FROM daily_balances
WHERE cumulative_balance <0;