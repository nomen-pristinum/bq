
create table ethereum.balance_deltas
(
    tx_date         Date,
    address_bin     String,
    currency_id     UInt32,
    value           Float64,
    block           UInt32
)
ENGINE = MergeTree()
PARTITION BY tx_date
ORDER BY (block, tx_date, currency_id, address_bin)
;

create materialized view ethereum.incoming_tx_mv
to ethereum.balance_deltas
as
    select
        tx_date,
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
        transfer_from_bin as address_bin,
        currency_id,
        -value,
        block
    from ethereum.transfers_tx_storage
    where transfer_from_bin <>
          '                    ' --miners
    and value <> 0
;
select count(*) from balance_deltas where address_bin = '                    ';

CREATE TABLE discrepancy_log
(
    address_bin String,
    detected_dtm DateTime,
    fixed_flag UInt8 DEFAULT 0,
    fixed_dtm DateTime NULL
)
ENGINE = MergeTree()
ORDER BY (address_bin, detected_dtm);

insert into discrepancy_log values (123, now(), null, null);

insert into discrepancy_log
SELECT DISTINCT address_bin, now(), null, null
FROM balance_deltas
WHERE block NOT IN (
    SELECT DISTINCT block
    FROM transfers_tx_storage
    );

select * from discrepancy_log;

delete from balance_deltas
where address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0 );






