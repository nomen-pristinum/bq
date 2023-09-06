CREATE TABLE ethereum.aggregated_deltas
(
    tx_date Date,
    address_bin String,
    currency_id UInt32,
    balance_delta Float64
) ENGINE = SummingMergeTree(balance_delta)
PARTITION BY toYYYYMM(tx_date)
ORDER BY (tx_date, address_bin, currency_id);

CREATE MATERIALIZED VIEW ethereum.aggregated_deltas_mv
TO ethereum.aggregated_deltas
AS
SELECT
    tx_date,
    address_bin,
    currency_id,
    value as balance_delta
FROM ethereum.balance_deltas;
--GROUP BY tx_date, address_bin, currency_id;

CREATE TABLE ethereum.aggregated_deltas_2 --replica to enable efficient cumulative JOINs
(
    tx_date Date,
    address_bin String,
    currency_id UInt32,
    balance_delta Float64
) ENGINE = SummingMergeTree(balance_delta)
PARTITION BY toYYYYMM(tx_date)
ORDER BY (tx_date, address_bin, currency_id);

CREATE MATERIALIZED VIEW ethereum.aggregated_deltas_2_mv
TO ethereum.aggregated_deltas_2
AS
SELECT *
FROM ethereum.aggregated_deltas_2;
-- Executes alphabetically before cumulative_balances_history_mv
-- which uses it but places no trigger on its table. This ensures
-- the latter has consistent data.