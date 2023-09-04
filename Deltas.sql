CREATE TABLE ethereum.aggregated_deltas
(
    tx_date Date,
    address_bin String,
    currency_id UInt32,
    balance AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(tx_date)
ORDER BY (tx_date, address_bin, currency_id);

CREATE MATERIALIZED VIEW ethereum.aggregated_deltas_mv
TO ethereum.aggregated_deltas
AS
SELECT
    tx_date,
    address_bin,
    currency_id,
    sumState(value) as balance
FROM ethereum.balance_deltas
GROUP BY tx_date, address_bin, currency_id;

CREATE VIEW ethereum.aggregated_deltas_vw
AS
SELECT
    tx_date,
    address_bin,
    currency_id,
    sumMerge(balance) as balance_delta
FROM ethereum.aggregated_deltas
GROUP BY tx_date, address_bin, currency_id;