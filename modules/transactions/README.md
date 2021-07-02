# Ignite transactions
This module provides transactions support for cross partition operations. Using the transactions, such operations are
executed in atomic way (either all changes all applied, or nothing at all) with a strong isolation.

Transactions support is supposed to be icremental. In the first approach, we are trying to put existing ideas from
ignite 2 to the new replication infrastructure. In the next phases, MVCC support should be added to avoid blocking reads
and some other optimization, like parallel commits from <sup id="a1">[1](#f1)</sup>

# Transaction protocol design
In high level, we utilize 2 phase locking (2PL) for a concurrency control, 2 phase commit (2PC) as an atomic commitment 
protocol, in conjunction with WAIT_DIE deadlock prevention, described in <sup id="a2">[2](#f2)</sup>. 
This implementation is very close to Ignite 2 optimistic serializable mode. 
Additional goals are: 
1) retain only strong isolation 
2) support for SQL 
3) utilize new common replication infrastructure based on RAFT.

# Two phase commit
This protocol is responsible for atomic commitment (all or nothing) tx guraranties.
Each update is **pre-written** to a replication groups on first phase (and replicated to a majority).
As soon as all updates are pre-written, it's safe to commit.
This slightly differs from ignite 2, because there is no PREPARED state.

# Two phase locking
2PL states the transaction constist of growing phase, where locks are acquired, and shrinking phase where locks are released.
A tx acquires shared locks for reads and exclusive locks for writes.
It's possible to lock for read in exclusive mode (select for update semantics)
Locks for different keys can be acquired in parallel.
Shared lock is obtained on DN-read operation.
Exclusive lock is obtained on DN-prewrite operation.

(DN - data node)

# Lock manager
Locking functionality is implemented by LockManager.
Each **leaseholder** for a partition replication group deploys an instance of LockManager. 
All reads and writes go through the **leaseholder**. Only raft leader for some term can become a **leaseholder**.
It's important what no two different leaseholder intervals can overlap for the same group.
Current leasholder map can be loaded from metastore (watches can be used for a fast notification about leaseholder change).

The lockmanager should keep locks in the offheap to reduce GC pressure, but can be heap based in the first approach.

# Locking precausion
LockManager has a volatile state, so some precausions must be taken before locking the keys due to possible node restarts.
Before taking a lock, LockManager should consult a tx state for the key (by reading it's metadata if present and looking into txstate map).
If a key is enlisted in transaction and wait is possible according to tx priority, lock cannot be taken immediately.

# Tx coordinator
Tx coordinator is assigned in a random fashion from a list of allowed nodes. They can be dedicated nodes or same as data nodes.
They are responsible for id assignment and failover handling if some nodes from tx topology have failed. 
Knows full tx topology.

It's possible to live without dedicated coordinators, but it will make client tx logic more complex and prone to coordinator failures.
The drawback of this is a additional hop for each tx op.
But from the architecure point of view having the dedicated coordinators is definetely more robust.

# Deadlock prevention
Deadlock prevention in WAIT_DIE mode - uses priorities to decide which tx should be restarted.
Each transaction is assigned a unique globally comparable timestamp (for example UUID), which defines tx priority.
If T1 has lower priority when T2 (T1 is younger) it can wait for lock, otherwise it's restarted keeping it's timestamp.
committing transaction can't be restarted.
Deadlock detection is not an option due to huge computation resources requirement and no real-time guaranties.

# Tx map
Each node maintains a persistent tx map:

txid -> timestamp|txstate(PENDING|ABORTED|COMMITED)

This map is used for a failover. Oldest entries in txid map must be cleaned to avoid unlimited grow.

# Data format
A row is stored in key-value database with additional attached metadata for referencing associated tx.

# Write tx example.
Assume the current row is: key -> oldvalue

The steps to update a row:

1. acquire exclusive lock on key on prewrite

2. remove key -> oldvalue<br/>
   set key -> newvalue [txid] // Inserted row has a special metadata containing transaction id it's enlisted in.<br/>
   set txid + key -> oldvalue (for aborting purposes)

3. on commit:<br/>
   set txid -> commited<br/>
   release exclusive lock<br/>
   async clear garbage

4. on abort:<br/>
   set txid -> aborted<br/>
   remove key -> newvalue<br/>
   set key -> oldvalue<br/>
   release exclusive lock<br/>
   async clear garbage

# SQL and indexes.

We assume only row level locking in the first approach, which gives us a repeatable_read isolation.

When the SQL query is executed, it acquires locks while the the data is collected on data nodes.

Locks are acquired lazily as result set is consumed.

The locking rules are same as for get/put operations.

Then values are removed from indexes on step 2, they are written as tombstones to avoid read inconsistency and should be 
cleaned up after tx finish.

# Failover handling
Failover protocol is similar to Ignite 2 with a main difference: until tx is sure it can commit or rollback, it holds
its locks. This means in the case of split-brain, some keys will be locked until split-brain situation is resolved and
tx recovery protocol will converge.

## Leaserholder fail
If a tx is not started to COMMIT, the coordinator reverts a transaction on remaining leaseholders.
Then a new leasholder is elected, it check for its pending transactions and asks a coordinator if it's possible to commit.

## Coordinator fail
Broadcast recovery (various strategies are possible: via gossip or dedicated node) is necessary (because we don't have 
full tx topology on each enlisted node - because it's unknown until commit). All nodes are requested about local txs state. 
If at least one is commiting, it's safe to commit.

<em id="f1">[1]</em> CockroachDB: The Resilient Geo-Distributed SQL Database, 2020 [↩](#a1)<br/>
<em id="f2">[2]</em> Concurrency Control in Distributed Database Systems, 1981 [↩](#a2)
 