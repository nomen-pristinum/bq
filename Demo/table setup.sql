/*drop table aggregated_deltas_after_delete;

drop table aggregated_deltas_after_fix;

drop table aggregated_deltas_before;

drop table cumulative_balances_history_after_delete;

drop table cumulative_balances_history_after_fix;

drop table cumulative_balances_history_before;

drop table current_balances_after_delete;

drop table current_balances_after_fix;

drop table current_balances_before;*/



create table aggregated_deltas_before
(
    tx_date       Date,
    address_hex   String,
    currency_id   UInt32,
    balance_delta Float64
)
engine = MergeTree()
order by (tx_date, address_hex, currency_id);

create table aggregated_deltas_after_delete
(
    tx_date       Date,
    address_hex   String,
    currency_id   UInt32,
    balance_delta Float64
)
engine = MergeTree()
order by (tx_date, address_hex, currency_id);

create table aggregated_deltas_after_fix
(
    tx_date       Date,
    address_hex   String,
    currency_id   UInt32,
    balance_delta Float64
)
engine = MergeTree()
order by (tx_date, address_hex, currency_id);

create table current_balances_before
(
    address_hex String,
    currency_id UInt32,
    balance Float64
)
engine = MergeTree()
order by (address_hex, currency_id);

create table current_balances_after_delete
(
    address_hex String,
    currency_id UInt32,
    balance Float64
)
engine = MergeTree()
order by (address_hex, currency_id);

create table current_balances_after_fix
(
    address_hex String,
    currency_id UInt32,
    balance Float64
)
engine = MergeTree()
order by (address_hex, currency_id);

create table cumulative_balances_history_before
(
    tx_date       Date,
    address_hex   String,
    currency_id   UInt32,
    cumulative_balance Float64
)
engine = MergeTree()
order by (tx_date, address_hex, currency_id);

create table cumulative_balances_history_after_delete
(
    tx_date       Date,
    address_hex   String,
    currency_id   UInt32,
    cumulative_balance Float64
)
engine = MergeTree()
order by (tx_date, address_hex, currency_id);

create table cumulative_balances_history_after_fix
(
    tx_date       Date,
    address_hex   String,
    currency_id   UInt32,
    cumulative_balance Float64
)
engine = MergeTree()
order by (tx_date, address_hex, currency_id);