# Eventing API demo

This repo is a demo of a small Go microservice for use in an event-driven architecture.

The demonstration here is that you can build a synchronous API based on a relational database like Postgres and have it participate cleanly in a wider event based system without sacrificing initial synchronous development speed.

This is naturally only useful for synchronous user experiences on the edges of the system, or for very young architectures in which investment in a reliable, persistent event broker is expensive, especially when long or near-infinite retention is desired.

This is ideal for control plane user experiences where synchronous feedback for user requests is beneficial.

Note however that this doesn't preclude this service being used for other, low-scale (relatively) use-cases.

## The replication log gadgets

Given a Postgres table representing a Resource, assign each mutable row in the table a unique id and monotonic revision number along with its existing data fields.

For each Resource, create a companion ResourceChangeLog table with a row id serial, tombstone boolean, and all the same data fields as Resource minus any specific indexed fields.

During any insert, update, or delete operation to the Resource, perform the following steps:

1. Acquire an advisory lock on the ResourceChangeLog table.
2. Acquire the latest value from the row id sequence.
3. Add a new row to the ResourceChangeLog (mark it as tombstone if the rows are being deleted).
4. Additionally, emit a Postgres NOTIFY call.

### Performance and partitioning

This intentionally trades off write performance for monotonic reads of the ResourceChangeLog table: any new read of the ResourceChangeLog shares a common prefix with any prior read of the ResourceChangeLog table when sorted by the row id.

Depending on how much data your system contains, It's a good idea to partition these records by groups of row id, leaving old changes in older partitions. This has the advantage of serving any new queries for replication logs out of "hot" partitions, while old queries that are pulling the entire replication log may load "cold" partitions. It may be difficult to avoid this without republishing old change log entries back into the "hot" partition.

## Compacting the replication log

The ResourceChangeLog table always contains at least as many rows as the Resource table. It additionally contains a tombstone records for any items fully deleted from the Resource table as well as a number of previous revisions for rows that were deleted.

We thus have two compaction routines to perform:

1. On some interval, delete any old revisions of rows that are NOT the latest and were created more then N minutes/hours ago.
2. On some rare interval, delete any old tombstone rows that were created more than M minutes/hours/days ago. This could even be _never_, but in reality most systems can afford to drop old rows at some point.

This can be complex to do at scale since it generally requires compacting each row individually, however it can be fairly well optimised since you can identify which rows have been modified since last compaction and which ones have not.
