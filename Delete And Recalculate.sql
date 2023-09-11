--TO-DO
--currency ID
--remove aggregated deltas 2
--deal with current balances
select * from discrepancy_log;

delete from balance_deltas
where address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
and tx_time >= (
    select distinct block_time
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix
    limit 1 --safety
    ) ;

delete from aggregated_deltas
where address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
and tx_date = (select toDate(block_time)
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix);

insert into ethereum.aggregated_deltas
SELECT
    tx_date,
    address_bin,
    currency_id,
    value as balance_delta
FROM ethereum.balance_deltas
where address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
and tx_date = (select toDate(block_time)
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix);

delete from aggregated_deltas_2
where address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
and tx_date = (select toDate(block_time)
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix);

insert into ethereum.aggregated_deltas_2
SELECT
    tx_date,
    address_bin,
    currency_id,
    value as balance_delta
FROM ethereum.balance_deltas
where address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
and (select toDate(block_time)
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix);

delete from cumulative_balances_history
where address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
and tx_date >=  -- everything is polluted
                -- for the affected addresses
                -- past this point in time:
    (select toDate(block_time)
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix);

INSERT INTO cumulative_balances_history
SELECT
    d1.tx_date,
    d1.address_bin,
    d1.currency_id,
    sum(d2.balance_delta) as cumulative_balance
FROM ethereum.aggregated_deltas d1
JOIN ethereum.aggregated_deltas d2
ON d1.address_bin = d2.address_bin
AND d1.currency_id = d2.currency_id
WHERE d2.tx_date <= d1.tx_date
AND d1.tx_date >= (
    select toDate(block_time)
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
AND d1.address_bin in (
    select distinct address_bin
    from discrepancy_log
    where fixed_flag = 0
    and block = next_block_to_fix)
GROUP BY d1.tx_date, d1.address_bin, d1.currency_id
ORDER BY d1.address_bin, d1.tx_date;


alter table discrepancy_log
update fixed_flag = 1
where block = next_block_to_fix;

alter table discrepancy_log
update next_block_to_fix = (
    select min(block)
    from discrepancy_log
    WHERE fixed_flag = 0
)
where 1 = 1;



