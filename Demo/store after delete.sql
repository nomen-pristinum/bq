insert into aggregated_deltas_after_delete
select *
from aggregated_deltas_vw
where (address_hex, currency_id) in
      (select hex(transfer_to_bin) as address_hex,
              currency_id
       from example_delete
       union all
       select hex(transfer_from_bin) as address_hex,
              currency_id
       from example_delete
       where transfer_from_bin <>
             '                    ')
and tx_date >= (select min(tx_date)
       from example_delete)
order by tx_date asc;

insert into current_balances_after_delete
select hex(address_bin),
    currency_id,
    balance as balance from current_balances_vw
where (address_bin, currency_id) in
      (select (transfer_to_bin),
              currency_id
       from example_delete
       union all
       select (transfer_from_bin),
              currency_id
       from example_delete);

insert into cumulative_balances_history_after_delete
select tx_date,
       hex(address_bin) as adress_hex,
       currency_id,
       cumulative_balance
from cumulative_balances_history
where (address_bin, currency_id) in
      (select (transfer_to_bin),
              currency_id
       from example_delete
       union all
       select (transfer_from_bin),
              currency_id
       from example_delete)
and tx_date >= (select min(tx_date)
       from example_delete)
order by tx_date asc;

