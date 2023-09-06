CREATE TABLE IF NOT EXISTS discrepancy_log
(
    block UInt32,
    address_bin String,
    block_time DateTime,
    detected_dtm DateTime,
    fixed_flag UInt8 DEFAULT 0,
    fixed_dtm DateTime NULL,
    next_block_to_fix UInt32 NULL
)
ENGINE = MergeTree()
ORDER BY (address_bin, detected_dtm);


--delete from transfers_tx_storage where block = 622230;

insert into discrepancy_log
SELECT DISTINCT block, address_bin,
                min(tx_time) over (partition by block),
                now(), null, null, null
FROM balance_deltas
WHERE block NOT IN (
    SELECT DISTINCT block
    FROM transfers_tx_storage
    );

alter table discrepancy_log
update next_block_to_fix = (
    select min(block)
    from discrepancy_log
    WHERE fixed_flag = 0
)
where 1 = 1;