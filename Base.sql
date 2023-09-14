
create table ethereum.balance_deltas
(
    tx_date         Date,
    tx_time         DateTime,
    address_bin     String,
    currency_id     UInt32,
    value           Float64,
    block           UInt32
)
ENGINE = MergeTree()
PARTITION BY toStartOfMonth(tx_date)
ORDER BY (block, tx_date, currency_id, address_bin)
;

create materialized view ethereum.incoming_tx_mv
to ethereum.balance_deltas
as
    select
        tx_date,
        tx_time,
        transfer_to_bin as address_bin,
        currency_id,
        value,
        block
    from ethereum.transfers_tx_storage
    where value <> 0
;

create materialized view ethereum.outgoing_tx_mv
to ethereum.balance_deltas
as
    select
        tx_date,
        tx_time,
        transfer_from_bin as address_bin,
        currency_id,
        -value,
        block
    from ethereum.transfers_tx_storage
    where value <> 0 --happens more often and covers second 99%, keep it first
    and transfer_from_bin <>
          '                    ' --minting
;
--initialise balance_deltas
INSERT INTO balance_deltas
    select
        tx_date,
        tx_time,
        transfer_to_bin as address_bin,
        currency_id,
        value,
        block
    from ethereum.transfers_tx_storage
    where value <> 0
UNION ALL
    select
        tx_date,
        tx_time,
        transfer_from_bin as address_bin,
        currency_id,
        -value,
        block
    from ethereum.transfers_tx_storage
    where value <> 0 --happens more often and covers second 99%, keep it first
    and transfer_from_bin <>
          '                    ' --minting




