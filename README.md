# Ethereum Balance Tracking System

This system tracks balance changes on the Ethereum network, processes deltas, and maintains a history of cumulative balances.

v2.0 
- Now supports insertions of many correction blocks at once. The standard logic is correction-aware and handles the regular operation vs. data fixes accordingly without the user intervention.
- Current balances are now stored as well as the historical ones
- Overall performances is improved and all features work in milliseconds
- Some changes not thoroughly tested

The solution is organized into several SQL files, each serving a distinct purpose:

1. **Base.sql**: Setting up primary tables and views.
2. **Deltas.sql**: Aggregating balance changes.
3. **Cumulative.sql**: Generating cumulative balances based on aggregated deltas.
4. **Detect.sql**: Checking for discrepancies in the data.
5. **Delete And Recalculate.sql**: Rectifying any discrepancies detected.

How to use:
Setup. On top of existing transaction storage data execute Base, Deltas, and Cumulative in that order. This will create objects and initialise the tables. After this the system handles normal operations, i.e. processes latest blocks in the chain as they come.
Corrections. Delete the faulty block(s) from transaction storage and run Detect. This will create discrepancy_log with all the deleted blocks, metadata and datafix progress. After that execute Delete And Recalculate once per deleted block to bring the derived data up to date. Finally, insert the corrected block(s) into the transaction storage, it is picked up and reprocessed automatically.

Delete the faulty block(s).

## [Base.sql](./Base.sql)

This segment creates the fundamental table to track balance deltas. It then creates two materialized views capturing incoming and outgoing transactions.

## [Deltas.sql](./Deltas.sql)

This segment aggregates balance changes for daily transactions and ensures efficient data retrieval.

## [Cumulative.sql](./Cumulative.sql)

This segment provides aggregated data on the cumulative balances per address for any given day.

## [Detect.sql](./Detect.sql)

In case of any blocks missing in the transfer storage data, it's captured and logged in this segment. The table discrepancy_log stored all the blocks affected and their data. The oldest block is marked for further processing. Fixed flag should be maintaind for this approach to work. 

## [Delete And Recalculate.sql](./Delete%20And%20Recalculate.sql)

If discrepancies are found, this segment provides the mechanism to correct the balances by deleting balances and recalculating them. Fixes one block per run, oldest first. Uses the discrepancy_log table to fetch blocks to be fixed. Maintains the fixed flag and marks the next oldest block for processing automatically.

---

### Notes

- Ensure to execute scripts in the given order to maintain data integrity.
- Known issue: Works with a delay. SummingMergeTree approach alone doesn't provide sufficient performance on a single node home server (x64/SSD). To avoid duplicates in the first seconds or minutes after bigger loads, data needs to be accessed with SUM() GROUP BY instead of directly, which if injected in code breaks triggers and joins(?). 
- Not extensively tested with all the scenarios. It does handle multiple deleted blocks as an extra.
- To discuss: advantages of dropping partitions/ALTER TABLE DELETE over the lightweight DELETE currently used. 
