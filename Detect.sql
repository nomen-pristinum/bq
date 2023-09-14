CREATE TABLE IF NOT EXISTS discrepancy_log
(
    block UInt32,
    address_bin String,
    currency_id UInt32,
    block_time DateTime,
    detected_dtm DateTime,
    fixed_flag UInt8 DEFAULT 0,
    fixed_dtm DateTime NULL,
    next_block_to_fix UInt32 NULL
)
ENGINE = MergeTree()
ORDER BY (address_bin, detected_dtm);


--delete from transfers_tx_storage where block = 622230;

INSERT INTO discrepancy_log
SELECT DISTINCT block, address_bin, currency_id,
                min(tx_time) over (partition by block),
                now(), null, null, null
FROM balance_deltas
WHERE block NOT IN (
    SELECT DISTINCT block
    FROM transfers_tx_storage
    )
AND block NOT IN (
    SELECT DISTINCT block
    FROM discrepancy_log
    where fixed_flag = 0
    )
;

ALTER TABLE discrepancy_log
UPDATE next_block_to_fix = (
    SELECT min(block)
    FROM discrepancy_log
    WHERE fixed_flag = 0
)
WHERE 1 = 1;